const fs = require('fs');
const path = require('path');
let HttpsProxyAgent = null;
try {
  ({ HttpsProxyAgent } = require('https-proxy-agent'));
} catch (err) {
  HttpsProxyAgent = null;
}
const { Client } = require('@notionhq/client');
const Simperium = require('simperium');
require('dotenv').config();

const fetch =
  global.fetch ||
  (() => {
    try {
      return require('node-fetch');
    } catch (err) {
      throw new Error('fetch is not available. Use Node 18+ or install node-fetch.');
    }
  })();

const ENV_PATH = path.resolve(__dirname, '.env');
const STATE_FILE = path.resolve(__dirname, 'sync_state.json');

const APP_URL = 'https://app.simplenote.com';
const APP_ID = process.env.SIMPERIUM_APP_ID;
const SN_EMAIL = process.env.SN_EMAIL;

const notion = new Client({ auth: process.env.NOTION_TOKEN });

const PROP_SIMPLENOTE_ID = 'Simplenote ID';
const PROP_CONTENT = "What`s in your mind?";
const PROP_TAGS = 'Tags';

const CONTENT_CHUNK_SIZE = 1800;
const NOTION_API_DELAY = 500;
const DEBOUNCE_DELAY_MS = parseInt(process.env.DEBOUNCE_DELAY_MS || '3000', 10);
const MAX_QUEUE_SIZE = parseInt(process.env.MAX_QUEUE_SIZE || '1000', 10);
const USE_PAGE_BLOCKS = process.env.USE_PAGE_BLOCKS === 'true';

const RECONNECT_BASE_DELAY_MS = 2000;
const RECONNECT_MAX_DELAY_MS = 300000;

let state = { note_to_page: {}, note_to_modify: {}, note_to_deleted: {} };
let notionConfig = { parentObj: null, titlePropName: null };
let isShuttingDown = false;

const Logger = {
  info: (...args) => console.log(`[${new Date().toISOString()}] [INFO]`, ...args),
  warn: (...args) => console.warn(`[${new Date().toISOString()}] [WARN]`, ...args),
  error: (...args) => console.error(`[${new Date().toISOString()}] [ERROR]`, ...args),
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function loadState() {
  if (!fs.existsSync(STATE_FILE)) return;
  try {
    const parsed = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    state = {
      note_to_page: parsed.note_to_page || {},
      note_to_modify: parsed.note_to_modify || {},
      note_to_deleted: parsed.note_to_deleted || {},
    };
    Logger.info(`State loaded. Notes mapped: ${Object.keys(state.note_to_page).length}`);
  } catch (err) {
    Logger.warn('Failed to read state file. Using empty state.');
  }
}

function saveState() {
  const tmpPath = `${STATE_FILE}.tmp`;
  const payload = JSON.stringify(state, null, 2);
  try {
    fs.writeFileSync(tmpPath, payload, 'utf8');
    fs.renameSync(tmpPath, STATE_FILE);
  } catch (err) {
    Logger.warn('Atomic state save failed. Falling back to direct write.');
    try {
      fs.writeFileSync(STATE_FILE, payload, 'utf8');
    } catch (innerErr) {
      Logger.error('State save failed:', innerErr.message || innerErr);
    }
  }
}

function chunkText(text, size) {
  if (!text) return [];
  const chunks = [];
  for (let i = 0; i < text.length; i += size) {
    chunks.push(text.slice(i, i + size));
  }
  return chunks;
}

function buildProperties(noteId, content, tags = []) {
  const lines = (content || '').split('\n');
  const firstLine = lines.find((l) => l.trim().length > 0) || 'Untitled';
  const titleText = firstLine.substring(0, 80);

  const chunks = chunkText(content || '', CONTENT_CHUNK_SIZE);
  let richTextChunks = chunks.map((c) => ({ type: 'text', text: { content: c } }));

  if (richTextChunks.length > 100) {
    richTextChunks.length = 100;
    richTextChunks[99].text.content =
      richTextChunks[99].text.content.substring(0, CONTENT_CHUNK_SIZE - 3) + '...';
  }

  const props = {
    [notionConfig.titlePropName]: { title: [{ type: 'text', text: { content: titleText } }] },
    [PROP_CONTENT]: { rich_text: richTextChunks },
    [PROP_SIMPLENOTE_ID]: { rich_text: [{ type: 'text', text: { content: noteId } }] },
  };

  if (PROP_TAGS && Array.isArray(tags) && tags.length > 0) {
    props[PROP_TAGS] = {
      multi_select: tags.map((t) => ({ name: t.substring(0, 50) })),
    };
  }

  return props;
}

function buildBlocksFromContent(content) {
  const chunks = chunkText(content || '', 2000);
  return chunks.map((c) => ({
    object: 'block',
    type: 'paragraph',
    paragraph: { rich_text: [{ type: 'text', text: { content: c } }] },
  }));
}

async function initNotionConfig() {
  const rawDbId = process.env.NOTION_DATABASE_ID;
  if (!rawDbId) throw new Error('Missing NOTION_DATABASE_ID');
  if (!process.env.NOTION_TOKEN) throw new Error('Missing NOTION_TOKEN');
  if (!APP_ID) throw new Error('Missing SIMPERIUM_APP_ID');

  const dbIdMatch = rawDbId.match(/([0-9a-fA-F]{32})/);
  const dbId = dbIdMatch ? dbIdMatch[1] : rawDbId;

  Logger.info('Fetching Notion database schema...');
  const db = await executeWithRetry(() => notion.databases.retrieve({ database_id: dbId }));

  let parentObj = { database_id: dbId };
  let properties = db.properties;

  if (db.data_sources && db.data_sources.length > 0) {
    const dsId = db.data_sources[0].id;
    parentObj = { data_source_id: dsId };
    const ds = await executeWithRetry(() => notion.request({ path: `data_sources/${dsId}`, method: 'get' }));
    properties = ds.properties;
    Logger.info(`Detected data source: ${dsId}`);
  }

  let titlePropName = 'Name';
  for (const [key, val] of Object.entries(properties)) {
    if (val.type === 'title') {
      titlePropName = key;
      break;
    }
  }

  notionConfig = { parentObj, titlePropName };

  if (!properties[PROP_CONTENT]) {
    Logger.warn(`Property not found: ${PROP_CONTENT}`);
  }
  if (PROP_TAGS && properties[PROP_TAGS] && properties[PROP_TAGS].type !== 'multi_select') {
    Logger.warn(`Property ${PROP_TAGS} is not multi_select`);
  }

  Logger.info(`Notion config ready (title: ${titlePropName})`);
}

async function executeWithRetry(fn, maxRetries = 5) {
  let attempt = 0;
  while (attempt < maxRetries) {
    try {
      return await fn();
    } catch (error) {
      attempt += 1;
      if (error.code === 'rate_limited' || error.status === 429) {
        const retryAfterMs = error.headers?.get('retry-after')
          ? parseInt(error.headers.get('retry-after'), 10) * 1000
          : 1500 * Math.pow(2, attempt - 1);
        Logger.warn(`Notion rate limited. Retry in ${retryAfterMs}ms (${attempt}/${maxRetries})`);
        await sleep(retryAfterMs);
      } else {
        Logger.error(`Notion request failed (${attempt}/${maxRetries}):`, error.message || error);
        if (attempt >= maxRetries) throw error;
        await sleep(2000);
      }
    }
  }
}

const updateQueue = [];
let isProcessing = false;

function enqueueTask(task, label) {
  if (updateQueue.length >= MAX_QUEUE_SIZE) {
    Logger.warn(`Queue full (${MAX_QUEUE_SIZE}). Dropping task: ${label || 'unknown'}`);
    return;
  }
  updateQueue.push(task);
  processQueue();
}

async function processQueue() {
  if (isProcessing) return;
  isProcessing = true;

  while (updateQueue.length > 0) {
    const task = updateQueue.shift();
    try {
      await task();
    } catch (err) {
      Logger.error('Queue task failed:', err.message || err);
    }
    saveState();
    await sleep(NOTION_API_DELAY);
  }

  isProcessing = false;
  if (isShuttingDown) {
    Logger.info('Queue drained. Exiting.');
    process.exit(0);
  }
}

const pendingNotes = new Map();
const debounceMap = new Map();

function queueNotionSync(id, note) {
  if (isShuttingDown) return;
  pendingNotes.set(id, note);
  if (debounceMap.has(id)) clearTimeout(debounceMap.get(id));

  const timer = setTimeout(() => {
    debounceMap.delete(id);
    const latest = pendingNotes.get(id);
    pendingNotes.delete(id);
    if (!latest) return;
    enqueueTask(() => syncSingleNote(id, latest), `note ${id}`);
  }, DEBOUNCE_DELAY_MS);

  debounceMap.set(id, timer);
}

function extractVersion(note) {
  const candidates = [note?.version, note?.modificationDate, note?.modified, note?.last_modified];
  for (const val of candidates) {
    if (typeof val === 'number' && !Number.isNaN(val)) return val;
    if (typeof val === 'string' && val.trim() !== '' && !Number.isNaN(Number(val))) return Number(val);
  }
  return null;
}

async function syncSingleNote(id, note) {
  const version = extractVersion(note);
  const lastVersion = state.note_to_modify[id]?.version;
  if (version !== null && lastVersion !== undefined && version <= lastVersion) {
    Logger.info(`[Skip] Stale update: ${id}`);
    return;
  }

  if (note.deleted) {
    await archiveNote(id);
    state.note_to_deleted[id] = Date.now();
    delete state.note_to_modify[id];
    return;
  }

  const content = note.content || '';
  const tags = note.tags || [];
  const properties = buildProperties(id, content, tags);
  const blocks = USE_PAGE_BLOCKS ? buildBlocksFromContent(content) : null;
  const existingPageId = state.note_to_page[id];

  await executeWithRetry(async () => {
    if (existingPageId) {
      try {
        await notion.pages.update({ page_id: existingPageId, properties });
        if (USE_PAGE_BLOCKS) await replacePageBlocks(existingPageId, blocks);
        Logger.info(`[Update] ${id}`);
      } catch (e) {
        if (e.status === 400 || e.status === 404) {
          Logger.warn(`Missing page. Re-creating: ${id}`);
          await createNewPage(id, properties, blocks);
        } else {
          throw e;
        }
      }
    } else {
      await createNewPage(id, properties, blocks);
    }
  });

  if (version !== null) {
    state.note_to_modify[id] = { version, synced_at: Date.now() };
  } else {
    state.note_to_modify[id] = { version: Date.now(), synced_at: Date.now() };
  }
}

async function createNewPage(id, properties, blocks) {
  const res = await notion.pages.create({
    parent: notionConfig.parentObj,
    properties,
    children: USE_PAGE_BLOCKS ? blocks : undefined,
  });
  state.note_to_page[id] = res.id;
  Logger.info(`[Create] ${id}`);
}

async function archiveNote(id) {
  const pageId = state.note_to_page[id];
  if (!pageId) {
    Logger.warn(`Delete skipped (not found): ${id}`);
    return;
  }
  await executeWithRetry(() => notion.pages.update({ page_id: pageId, archived: true }));
  delete state.note_to_page[id];
  Logger.info(`[Archive] ${id}`);
}

async function replacePageBlocks(pageId, blocks) {
  if (!blocks || blocks.length === 0) return;

  let cursor = undefined;
  do {
    const res = await executeWithRetry(() =>
      notion.blocks.children.list({ block_id: pageId, start_cursor: cursor })
    );
    cursor = res.has_more ? res.next_cursor : undefined;
    for (const child of res.results) {
      await executeWithRetry(() => notion.blocks.update({ block_id: child.id, archived: true }));
    }
  } while (cursor);

  const chunkSize = 100;
  for (let i = 0; i < blocks.length; i += chunkSize) {
    const batch = blocks.slice(i, i + chunkSize);
    await executeWithRetry(() =>
      notion.blocks.children.append({ block_id: pageId, children: batch })
    );
  }
}

function parseTokenCookie(setCookieList) {
  for (const cookie of setCookieList) {
    if (cookie.toLowerCase().startsWith('token=')) {
      return cookie.match(/^token=([^;]+)/i)?.[1] || '';
    }
  }
  return null;
}

let tokenRefreshInFlight = null;

async function renewSimperiumToken() {
  if (tokenRefreshInFlight) return tokenRefreshInFlight;
  tokenRefreshInFlight = (async () => {
    Logger.info('Refreshing Simplenote token...');
    const currentToken = process.env.SIMPERIUM_TOKEN || '';
    const proxyUrl = process.env.HTTPS_PROXY || process.env.HTTP_PROXY || '';
    const cookieHeader = SN_EMAIL
      ? `token=${currentToken}; email=${encodeURIComponent(SN_EMAIL)}`
      : `token=${currentToken}`;

    if (proxyUrl && !HttpsProxyAgent) {
      Logger.warn('Proxy URL set but https-proxy-agent is missing. Request will bypass proxy.');
    }

    const response = await fetch(APP_URL, {
      method: 'GET',
      agent: proxyUrl && HttpsProxyAgent ? new HttpsProxyAgent(proxyUrl) : undefined,
      headers: {
        Cookie: cookieHeader,
        'User-Agent':
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/145.0.0.0 Safari/537.36',
      },
    });

    const setCookieList = response.headers.raw()['set-cookie'] || [];
    const newToken = parseTokenCookie(setCookieList);
    if (!newToken) throw new Error('Token not found in response');

    const oldText = fs.readFileSync(ENV_PATH, 'utf8');
    const newText = /^SIMPERIUM_TOKEN=.*$/m.test(oldText)
      ? oldText.replace(/^SIMPERIUM_TOKEN=.*$/m, `SIMPERIUM_TOKEN=${newToken}`)
      : `${oldText.trimEnd()}\nSIMPERIUM_TOKEN=${newToken}\n`;

    fs.writeFileSync(ENV_PATH, newText, 'utf8');
    process.env.SIMPERIUM_TOKEN = newToken;
    Logger.info('Token refreshed and written to .env');
    return newToken;
  })();

  try {
    return await tokenRefreshInFlight;
  } finally {
    tokenRefreshInFlight = null;
  }
}

let simperiumClient = null;
let connectionId = 0;
let reconnectAttempt = 0;
let reconnectTimer = null;

function resetBackoff() {
  reconnectAttempt = 0;
}

function computeBackoffDelay() {
  const base = Math.min(
    RECONNECT_MAX_DELAY_MS,
    RECONNECT_BASE_DELAY_MS * Math.pow(2, reconnectAttempt)
  );
  const jitter = Math.floor(Math.random() * 1000);
  return base + jitter;
}

function scheduleReconnect(reason) {
  if (isShuttingDown) return;
  if (reconnectTimer) return;

  const delay = computeBackoffDelay();
  Logger.warn(`Scheduling reconnect in ${delay}ms (${reason || 'unknown'})`);
  reconnectTimer = setTimeout(async () => {
    reconnectTimer = null;
    reconnectAttempt += 1;
    try {
      const token = process.env.SIMPERIUM_TOKEN || (await renewSimperiumToken());
      await connectSimperium(token);
    } catch (err) {
      Logger.error('Reconnect attempt failed:', err.message || err);
      scheduleReconnect('reconnect failed');
    }
  }, delay);
}

async function closeSimperium() {
  if (!simperiumClient) return;
  try {
    if (typeof simperiumClient.disconnect === 'function') {
      simperiumClient.disconnect();
    } else if (simperiumClient.socket && typeof simperiumClient.socket.close === 'function') {
      simperiumClient.socket.close();
    }
  } catch (err) {
    Logger.warn('Error closing client:', err.message || err);
  } finally {
    simperiumClient = null;
  }
}

async function connectSimperium(activeToken) {
  await closeSimperium();
  const localConnectionId = ++connectionId;
  Logger.info('Connecting to Simplenote WebSocket...');

  simperiumClient = new Simperium.Client(APP_ID, activeToken);

  simperiumClient.on('unauthorized', async () => {
    if (localConnectionId !== connectionId) return;
    Logger.warn('Unauthorized. Refreshing token...');
    try {
      const renewedToken = await renewSimperiumToken();
      resetBackoff();
      await connectSimperium(renewedToken);
    } catch (err) {
      Logger.error('Token refresh failed:', err.message || err);
      scheduleReconnect('token refresh failed');
    }
  });

  const noteBucket = simperiumClient.bucket('note');

  noteBucket.on('ready', () => {
    if (localConnectionId !== connectionId) return;
    resetBackoff();
    Logger.info('WebSocket ready. Listening for updates...');
  });

  noteBucket.on('update', (id, note) => {
    if (localConnectionId !== connectionId) return;
    queueNotionSync(id, note);
  });

  noteBucket.on('error', (err) => {
    if (localConnectionId !== connectionId) return;
    Logger.error('Bucket error:', err);
    scheduleReconnect('bucket error');
  });
}

async function startRealtimeSync() {
  await initNotionConfig();
  loadState();

  let token = process.env.SIMPERIUM_TOKEN;
  if (!token) {
    Logger.info('No token found. Attempting initial refresh...');
    token = await renewSimperiumToken();
  }

  await connectSimperium(token);
}

function isRecoverableSocketError(err) {
  const message = (err && err.message) || String(err);
  return message.includes('cannot call send() while not connected');
}

process.on('unhandledRejection', (reason) => {
  Logger.error('Unhandled promise rejection:', reason);
});

process.on('uncaughtException', (err) => {
  if (isRecoverableSocketError(err)) {
    Logger.warn('Socket not connected. Triggering reconnect.');
    scheduleReconnect('socket not connected');
    return;
  }
  Logger.error('Uncaught exception:', err);
  saveState();
  process.exit(1);
});

const shutdownHandler = async () => {
  Logger.info('Shutdown signal received. Draining queue...');
  isShuttingDown = true;
  for (const timer of debounceMap.values()) clearTimeout(timer);
  debounceMap.clear();
  pendingNotes.clear();

  if (updateQueue.length === 0 && !isProcessing) {
    saveState();
    await closeSimperium();
    Logger.info('Safe exit.');
    process.exit(0);
  } else {
    Logger.warn(`Waiting for ${updateQueue.length} queued tasks...`);
    setTimeout(async () => {
      Logger.error('Shutdown timeout. Forcing exit.');
      saveState();
      await closeSimperium();
      process.exit(1);
    }, 15000);
  }
};

process.on('SIGINT', shutdownHandler);
process.on('SIGTERM', shutdownHandler);

startRealtimeSync().catch((err) => {
  Logger.error('Service failed to start:', err.message || err);
  process.exit(1);
});
