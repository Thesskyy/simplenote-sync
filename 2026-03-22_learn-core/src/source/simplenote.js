const Simperium = require("simperium");
const db = require("../db");
const log = require("../logger");
const {
  extractVersion,
  shouldAcceptIncomingVersion,
  normalizeTimestampToIso,
} = require("../schema");
const config = require("../config");

const insertEventStmt = db.prepare(`
  INSERT INTO note_events (note_id, source_version, event_type, received_at, raw_note_json)
  VALUES (@note_id, @source_version, @event_type, @received_at, @raw_note_json)
`);

const upsertNoteStmt = db.prepare(`
  INSERT INTO notes (
    note_id, content, tags_json, deleted,
    source_version, source_created_at, source_updated_at, last_event_at, stable_after, status
  )
  VALUES (
    @note_id, @content, @tags_json, @deleted,
    @source_version, @source_created_at, @source_updated_at, @last_event_at, @stable_after, 'DIRTY'
  )
  ON CONFLICT(note_id) DO UPDATE SET
    content = excluded.content,
    tags_json = excluded.tags_json,
    deleted = excluded.deleted,
    source_version = excluded.source_version,
    source_created_at = COALESCE(notes.source_created_at, excluded.source_created_at),
    source_updated_at = excluded.source_updated_at,
    last_event_at = excluded.last_event_at,
    stable_after = excluded.stable_after,
    status = 'DIRTY'
  WHERE
    excluded.source_version IS NULL
    OR notes.source_version IS NULL
    OR excluded.source_version > notes.source_version
`);

const txSaveEvent = db.transaction((payload) => {
  insertEventStmt.run(payload);
  return upsertNoteStmt.run(payload);
});

const getCurrentVersionStmt = db.prepare(`
  SELECT source_version
  FROM notes
  WHERE note_id = ?
`);

function buildRawEventJson(note) {
  const raw = JSON.stringify(note || {});
  if (raw.length <= config.maxRawEventBytes) return raw;

  return JSON.stringify({
    __truncated: true,
    originalBytes: raw.length,
    preview: raw.slice(0, config.maxRawEventBytes),
  });
}

function saveIncomingNote(noteId, note) {
  const now = new Date();
  const version = extractVersion(note);
  const sourceCreatedAt = normalizeTimestampToIso(
    note?.creationDate ?? note?.created ?? note?.created_at,
  );
  const existing = getCurrentVersionStmt.get(noteId);

  if (!shouldAcceptIncomingVersion(existing?.source_version ?? null, version)) {
    log.warn(`ignored stale note event: ${noteId} version=${version}`);
    return;
  }

  const quietWindowMs = note?.deleted
    ? config.deleteQuietWindowMs
    : config.quietWindowMs;
  const stableAfter = new Date(now.getTime() + quietWindowMs);

  const upsertResult = txSaveEvent({
    note_id: noteId,
    content: note?.content || "",
    tags_json: JSON.stringify(Array.isArray(note?.tags) ? note.tags : []),
    deleted: note?.deleted ? 1 : 0,
    source_version: version,
    source_created_at: sourceCreatedAt,
    source_updated_at: now.toISOString(),
    last_event_at: now.toISOString(),
    stable_after: stableAfter.toISOString(),
    event_type: note?.deleted ? "delete" : "update",
    received_at: now.toISOString(),
    raw_note_json: buildRawEventJson(note),
  });

  if (version != null && upsertResult.changes === 0) {
    log.warn(`ignored stale note event: ${noteId} version=${version}`);
  }
}

function startSimplenoteListener() {
  const client = new Simperium.Client(
    config.simperiumAppId,
    config.simperiumToken,
  );

  let unauthorizedTriggered = false;

  client.on("unauthorized", () => {
    if (unauthorizedTriggered) return;
    unauthorizedTriggered = true;

    log.error(
      `Simperium unauthorized; exiting in ${config.unauthorizedExitDelayMs}ms for supervisor recovery`,
    );

    setTimeout(() => {
      process.exit(1);
    }, config.unauthorizedExitDelayMs);
  });

  const noteBucket = client.bucket("note");
  let sourceEventCount = 0;

  noteBucket.on("ready", () => {
    log.info("Simperium ready, listening note updates...");
  });

  noteBucket.on("update", (id, note) => {
    saveIncomingNote(id, note);
    sourceEventCount += 1;
    if (
      config.sourceEventInfoEvery > 0 &&
      sourceEventCount % config.sourceEventInfoEvery === 0
    ) {
      log.info(`saved note events: ${sourceEventCount} (latest: ${id})`);
    }
  });

  noteBucket.on("error", (err) => {
    log.error("bucket error:", err);
  });

  return client;
}

module.exports = { startSimplenoteListener };
