const fs = require('fs');
const path = require('path');
const fetch = require('node-fetch');
const { HttpsProxyAgent } = require('https-proxy-agent');
const { Client } = require('@notionhq/client');
const Simperium = require('simperium');
require('dotenv').config();

// =========================
// 1. 全局配置与状态
// =========================
const ENV_PATH = path.resolve(__dirname, '.env');
const STATE_FILE = path.resolve(__dirname, 'sync_state.json');

const APP_URL = 'https://app.simplenote.com';
const APP_ID = process.env.SIMPERIUM_APP_ID;
const APP_KEY = process.env.SIMPERIUM_APP_KEY; // 如果有用到账号密码登录则需要
const SN_EMAIL = process.env.SN_EMAIL;

const notion = new Client({ auth: process.env.NOTION_TOKEN });

// Notion 属性映射配置
const PROP_SIMPLENOTE_ID = "Simplenote ID";
const PROP_CONTENT = "What`s in your mind?";
const PROP_TAGS = "Tags"; // 请确保 Notion 数据库中有这个 Multi-select 类型的属性，不需要可留空
const CONTENT_CHUNK_SIZE = 1800;
const NOTION_API_DELAY = 500; 

let state = { note_to_page: {}, note_to_modify: {} };
let notionConfig = { parentObj: null, titlePropName: null };
let isShuttingDown = false; // 优雅退出标志

// =========================
// 2. 日志与工具函数
// =========================
const Logger = {
  info: (...args) => console.log(`[${new Date().toISOString()}] [INFO]`, ...args),
  warn: (...args) => console.warn(`[${new Date().toISOString()}] [WARN]`, ...args),
  error: (...args) => console.error(`[${new Date().toISOString()}] [ERROR]`, ...args),
};

function loadState() {
  if (fs.existsSync(STATE_FILE)) {
    try {
      state = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
      if (!state.note_to_page) state = { note_to_page: {}, note_to_modify: {} };
      Logger.info(`状态已加载，当前映射笔记数: ${Object.keys(state.note_to_page).length}`);
    } catch (e) {
      Logger.warn('读取 state 文件失败，初始化为空状态');
    }
  }
}

function saveState() {
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2), 'utf8');
}

function chunkText(text, size) {
  if (!text) return [];
  const chunks = [];
  for (let i = 0; i < text.length; i += size) {
    chunks.push(text.slice(i, i + size));
  }
  return chunks;
}

// 提取标题、正文与标签
function buildProperties(noteId, content, tags = []) {
  const lines = content.split('\n');
  const firstLine = lines.find(l => l.trim().length > 0) || 'Untitled';
  const titleText = firstLine.substring(0, 80);

  let chunks = chunkText(content, CONTENT_CHUNK_SIZE);
  let richTextChunks = chunks.map(c => ({ type: 'text', text: { content: c } }));
  
  if (richTextChunks.length > 100) {
    richTextChunks.length = 100;
    richTextChunks[99].text.content = richTextChunks[99].text.content.substring(0, CONTENT_CHUNK_SIZE - 3) + '...';
  }

  const props = {
    [notionConfig.titlePropName]: { title: [{ type: 'text', text: { content: titleText } }] },
    [PROP_CONTENT]: { rich_text: richTextChunks },
    [PROP_SIMPLENOTE_ID]: { rich_text: [{ type: 'text', text: { content: noteId } }] }
  };

  // 处理多选标签
  if (PROP_TAGS && Array.isArray(tags) && tags.length > 0) {
    props[PROP_TAGS] = {
      multi_select: tags.map(t => ({ name: t.substring(0, 50) })) // Notion 标签有限制长度
    };
  }

  return props;
}

// =========================
// 3. Notion 交互与重试队列
// =========================
async function initNotionConfig() {
  const rawDbId = process.env.NOTION_DATABASE_ID;
  const dbIdMatch = rawDbId.match(/([0-9a-fA-F]{32})/);
  const dbId = dbIdMatch ? dbIdMatch[1] : rawDbId;

  Logger.info('正在获取 Notion Database Schema...');
  const db = await executeWithRetry(() => notion.databases.retrieve({ database_id: dbId }));
  
  let parentObj = { database_id: dbId };
  let properties = db.properties;

  if (db.data_sources && db.data_sources.length > 0) {
    const dsId = db.data_sources[0].id;
    parentObj = { data_source_id: dsId };
    const ds = await executeWithRetry(() => notion.request({ path: `data_sources/${dsId}`, method: 'get' }));
    properties = ds.properties;
    Logger.info(`已探测到 Data Source ID: ${dsId}`);
  }

  let titlePropName = 'Name';
  for (const [key, val] of Object.entries(properties)) {
    if (val.type === 'title') {
      titlePropName = key;
      break;
    }
  }

  notionConfig = { parentObj, titlePropName };
  Logger.info(`Notion 配置加载完毕 (Title字段: ${titlePropName})`);
}

async function executeWithRetry(fn, maxRetries = 5) {
  let attempt = 0;
  while (attempt < maxRetries) {
    try {
      return await fn();
    } catch (error) {
      attempt++;
      if (error.code === 'rate_limited' || error.status === 429) {
        const retryAfterMs = error.headers?.get('retry-after') 
          ? parseInt(error.headers.get('retry-after'), 10) * 1000 
          : 1500 * Math.pow(2, attempt - 1);
        Logger.warn(`Notion 限流 (429)，等待 ${retryAfterMs}ms 后重试 (${attempt}/${maxRetries})...`);
        await new Promise(r => setTimeout(r, retryAfterMs));
      } else {
        Logger.error(`Notion 请求异常 (${attempt}/${maxRetries}):`, error.message || error);
        if (attempt >= maxRetries) throw error;
        await new Promise(r => setTimeout(r, 2000));
      }
    }
  }
}

const updateQueue = [];
let isProcessing = false;

async function processQueue() {
  if (isProcessing) return;
  isProcessing = true;

  while (updateQueue.length > 0) {
    const task = updateQueue.shift();
    try {
      await task();
    } catch (err) {
      Logger.error('队列任务最终失败放弃:', err.message || err);
    }
    saveState(); // 每处理完一个任务保存一次状态
    await new Promise(r => setTimeout(r, NOTION_API_DELAY));
  }

  isProcessing = false;
  
  if (isShuttingDown) {
    Logger.info('队列已清空，准备安全退出...');
    process.exit(0);
  }
}

const debounceMap = new Map();
const DEBOUNCE_DELAY_MS = 3000;

function queueNotionSync(id, note) {
  if (note.deleted) return;

  if (debounceMap.has(id)) clearTimeout(debounceMap.get(id));

  const timer = setTimeout(() => {
    debounceMap.delete(id);
    updateQueue.push(async () => {
      const content = note.content || '';
      const tags = note.tags || [];
      const properties = buildProperties(id, content, tags);
      const existingPageId = state.note_to_page[id];

      await executeWithRetry(async () => {
        if (existingPageId) {
          try {
            await notion.pages.update({ page_id: existingPageId, properties });
            Logger.info(`[Update] 同步成功: ${id}`);
          } catch (e) {
            if (e.status === 400 || e.status === 404) {
              Logger.warn(`页面丢失，重新创建: ${id}`);
              await createNewPage(id, properties);
            } else {
              throw e;
            }
          }
        } else {
          await createNewPage(id, properties);
        }
      });
    });
    processQueue();
  }, DEBOUNCE_DELAY_MS);

  debounceMap.set(id, timer);
}

async function createNewPage(id, properties) {
  const res = await notion.pages.create({
    parent: notionConfig.parentObj,
    properties: properties
  });
  state.note_to_page[id] = res.id;
  Logger.info(`[Create] 新建成功: ${id}`);
}

// =========================
// 4. Token 自动续期逻辑
// =========================
// (此处保持原有的 parseTokenCookie 逻辑)
function parseTokenCookie(setCookieList) {
  for (const cookie of setCookieList) {
    if (cookie.toLowerCase().startsWith('token=')) {
      return cookie.match(/^token=([^;]+)/i)?.[1] || '';
    }
  }
  return null;
}

async function renewSimperiumToken() {
  Logger.info('请求续期 Simplenote Token...');
  const currentToken = process.env.SIMPERIUM_TOKEN || '';
  const proxyUrl = process.env.HTTPS_PROXY || process.env.HTTP_PROXY || '';
  const cookieHeader = SN_EMAIL ? `token=${currentToken}; email=${encodeURIComponent(SN_EMAIL)}` : `token=${currentToken}`;

  const response = await fetch(APP_URL, {
    method: 'GET',
    agent: proxyUrl ? new HttpsProxyAgent(proxyUrl) : undefined,
    headers: {
      Cookie: cookieHeader,
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/145.0.0.0 Safari/537.36',
    },
  });

  const setCookieList = response.headers.raw()['set-cookie'] || [];
  const newToken = parseTokenCookie(setCookieList);

  if (!newToken) throw new Error('未在响应中找到 token Set-Cookie');

  const oldText = fs.readFileSync(ENV_PATH, 'utf8');
  let newText = /^SIMPERIUM_TOKEN=.*$/m.test(oldText)
    ? oldText.replace(/^SIMPERIUM_TOKEN=.*$/m, `SIMPERIUM_TOKEN=${newToken}`)
    : `${oldText.trimEnd()}\nSIMPERIUM_TOKEN=${newToken}\n`;
  
  fs.writeFileSync(ENV_PATH, newText, 'utf8');
  process.env.SIMPERIUM_TOKEN = newToken;
  Logger.info('Token 续期成功并已回写 .env');
  return newToken;
}

// =========================
// 5. WebSocket 核心与连接守护
// =========================
let simperiumClient = null;

async function startRealtimeSync() {
  await initNotionConfig();
  loadState();

  let token = process.env.SIMPERIUM_TOKEN;
  if (!token) {
    Logger.info('Token 不存在，尝试初次获取...');
    token = await renewSimperiumToken();
  }

  connectSimperium(token);
}

function connectSimperium(activeToken) {
  Logger.info('建立 Simplenote WebSocket 同步连接...');
  simperiumClient = new Simperium.Client(APP_ID, activeToken);
  
  simperiumClient.on('unauthorized', async () => {
    Logger.warn('认证失效 (unauthorized)，开始尝试自动续期...');
    try {
      const renewedToken = await renewSimperiumToken();
      setTimeout(() => connectSimperium(renewedToken), 2000); // 延时重连防死循环
    } catch (err) {
      Logger.error('自动续期失败，程序即将退出以便外部守护程序重启:', err);
      process.exit(1);
    }
  });

  const noteBucket = simperiumClient.bucket('note');

  noteBucket.on('ready', () => {
    Logger.info('✅ WebSocket 连接已就绪，正在实时监听笔记更新...');
  });

  noteBucket.on('update', (id, note) => {
    Logger.info(`捕获变更 -> ID: ${id}`);
    queueNotionSync(id, note);
  });

  noteBucket.on('error', (err) => {
    Logger.error('WebSocket 错误:', err);
    // 依赖进程守护工具（如PM2）在遇到严重网络错误抛出异常时自动重启
  });
}

// =========================
// 6. 进程守护与优雅退出
// =========================
// 捕获未处理的 Promise 拒绝，防止服务静默死掉
process.on('unhandledRejection', (reason, promise) => {
  Logger.error('未捕获的 Promise 异常:', reason);
});
process.on('uncaughtException', (err) => {
  Logger.error('未捕获的系统异常:', err);
  saveState();
  process.exit(1); // 抛出异常退出，由守护进程拉起
});

// 处理 Ctrl+C 或 Docker/PM2 停止信号
const shutdownHandler = () => {
  Logger.info('收到停止信号，正在执行优雅退出...');
  isShuttingDown = true;
  
  if (updateQueue.length === 0 && !isProcessing) {
    saveState();
    Logger.info('队列为空，状态已保存，安全退出。');
    process.exit(0);
  } else {
    Logger.warn(`等待队列中的 ${updateQueue.length} 个任务处理完毕后再退出...`);
    // 如果卡死，强制 10 秒后退出
    setTimeout(() => {
      Logger.error('等待超时，强制保存状态并退出。');
      saveState();
      process.exit(1);
    }, 10000);
  }
};

process.on('SIGINT', shutdownHandler);
process.on('SIGTERM', shutdownHandler);

// 启动入口
startRealtimeSync().catch(err => {
  Logger.error('服务启动失败:', err);
  process.exit(1);
});