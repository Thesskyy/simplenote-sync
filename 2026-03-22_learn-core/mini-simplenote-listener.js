require("dotenv").config();
const Simperium = require("simperium");

const APP_ID = process.env.SIMPERIUM_APP_ID;
const TOKEN = process.env.SIMPERIUM_TOKEN;

if (!APP_ID) {
  console.error("缺少 SIMPERIUM_APP_ID");
  process.exit(1);
}

if (!TOKEN) {
  console.error("缺少 SIMPERIUM_TOKEN");
  process.exit(1);
}

// 这是“客户端对象”
// 你可以把它理解成：程序和 Simperium 服务器之间的总连接管理器
const client = new Simperium.Client(APP_ID, TOKEN);

// unauthorized = token 失效 / 认证失败
client.on("unauthorized", () => {
  console.error(`[${new Date().toISOString()}] unauthorized: token 可能失效了`);
});

// 拿到 note bucket
// 你可以把 bucket 理解成“笔记这一类对象的频道 / 分组”
const noteBucket = client.bucket("note");

// ready = 连接完成，开始进入监听状态
noteBucket.on("ready", () => {
  console.log(
    `[${new Date().toISOString()}] ready: 已连接，正在监听 note 更新`,
  );
});

// update = 收到某条 note 的变化
noteBucket.on("update", (id, note) => {
  console.log("----------------------------------------");
  console.log(`[${new Date().toISOString()}] update`);
  console.log("note id =", id);

  // 为了 0 基础易读，只打印最常用字段
  console.log("deleted =", !!note?.deleted);
  console.log("tags =", Array.isArray(note?.tags) ? note.tags : []);
  console.log("content =");
  console.log(note?.content || "(empty)");
});

// error = bucket 层出错
noteBucket.on("error", (err) => {
  console.error(`[${new Date().toISOString()}] bucket error:`, err);
});

// 让程序不要立刻退出
console.log("程序已启动，等待服务器事件...");
