require("dotenv").config();

function must(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env: ${name}`);
  return v;
}

function toNumber(name, defaultValue, options = {}) {
  const { min = Number.NEGATIVE_INFINITY, max = Number.POSITIVE_INFINITY } =
    options;
  const raw = process.env[name];
  const parsed = Number(raw ?? defaultValue);
  if (!Number.isFinite(parsed)) return defaultValue;
  if (parsed < min) return min;
  if (parsed > max) return max;
  return parsed;
}

function toBoolean(name, defaultValue) {
  const raw = process.env[name];
  if (raw == null) return defaultValue;
  return String(raw).toLowerCase() === "true";
}

module.exports = {
  simperiumAppId: must("SIMPERIUM_APP_ID"),
  simperiumToken: must("SIMPERIUM_TOKEN"),
  snEmail: process.env.SN_EMAIL || "",

  notionToken: must("NOTION_TOKEN"),
  notionDatabaseId: must("NOTION_DATABASE_ID"),

  quietWindowMs: toNumber("QUIET_WINDOW_MS", 15000, { min: 1000, max: 300000 }),
  deleteQuietWindowMs: toNumber("DELETE_QUIET_WINDOW_MS", 5000, {
    min: 1000,
    max: 300000,
  }),
  dispatchIntervalMs: toNumber("DISPATCH_INTERVAL_MS", 2000, {
    min: 500,
    max: 60000,
  }),
  retryBaseMs: toNumber("RETRY_BASE_MS", 2000, { min: 500, max: 600000 }),
  retryMaxMs: toNumber("RETRY_MAX_MS", 300000, { min: 5000, max: 3600000 }),
  maxRetryCount: toNumber("MAX_RETRY_COUNT", 8, { min: 1, max: 50 }),
  noteEventsRetentionDays: toNumber("NOTE_EVENTS_RETENTION_DAYS", 30, {
    min: 0,
    max: 3650,
  }),
  noteEventsPurgeIntervalMs: toNumber(
    "NOTE_EVENTS_PURGE_INTERVAL_MS",
    3600000,
    {
      min: 60000,
      max: 86400000,
    },
  ),
  metricsLogEnabled: toBoolean("METRICS_LOG_ENABLED", true),
  metricsLogIntervalMs: toNumber("METRICS_LOG_INTERVAL_MS", 60000, {
    min: 10000,
    max: 3600000,
  }),
  sourceEventInfoEvery: toNumber("SOURCE_EVENT_INFO_EVERY", 20, {
    min: 0,
    max: 100000,
  }),
  maxRawEventBytes: toNumber("MAX_RAW_EVENT_BYTES", 65536, {
    min: 1024,
    max: 1048576,
  }),
  healthPort: toNumber("HEALTH_PORT", 0, { min: 0, max: 65535 }),
  unauthorizedExitDelayMs: toNumber("UNAUTHORIZED_EXIT_DELAY_MS", 3000, {
    min: 0,
    max: 60000,
  }),
  usePageBlocks: toBoolean("USE_PAGE_BLOCKS", false),
};
