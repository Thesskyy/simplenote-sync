const http = require("http");
const config = require("./config");
const log = require("./logger");
const { initNotionConfig } = require("./sink/notion");
const { startSimplenoteListener } = require("./source/simplenote");
const {
  startDispatcher,
  getDispatcherMetrics,
} = require("./workers/dispatcher");
const { startNoteEventsRetention } = require("./workers/noteEventsRetention");

const startedAtMs = Date.now();

function setupHealthServer() {
  if (!Number.isFinite(config.healthPort) || config.healthPort <= 0) return;

  const server = http.createServer((req, res) => {
    if (req.url !== "/healthz") {
      res.statusCode = 404;
      res.end("not found");
      return;
    }

    const payload = {
      status: "ok",
      uptimeSec: Math.floor((Date.now() - startedAtMs) / 1000),
      metrics: getDispatcherMetrics(),
    };

    res.setHeader("content-type", "application/json");
    res.end(JSON.stringify(payload));
  });

  server.listen(config.healthPort, () => {
    log.info(`health probe listening on :${config.healthPort} /healthz`);
  });
}

function setupMetricsLogger() {
  if (!config.metricsLogEnabled) {
    log.info("metrics logger disabled");
    return;
  }

  setInterval(() => {
    const uptimeSec = Math.floor((Date.now() - startedAtMs) / 1000);
    log.info("metrics", {
      uptimeSec,
      ...getDispatcherMetrics(),
    });
  }, config.metricsLogIntervalMs);
}

async function main() {
  await initNotionConfig();
  startSimplenoteListener();
  startDispatcher(config.dispatchIntervalMs);
  startNoteEventsRetention();
  setupHealthServer();
  setupMetricsLogger();
  log.info("service started");
}

main().catch((err) => {
  log.error("fatal:", err.message || err);
  process.exit(1);
});
