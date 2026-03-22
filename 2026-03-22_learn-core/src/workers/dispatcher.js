const db = require("../db");
const log = require("../logger");
const config = require("../config");
const { notionUpsert } = require("../sink/notion");
const { computeRetryDelayMs } = require("../retryPolicy");

const dispatcherMetrics = {
  loops: 0,
  selected: 0,
  skippedByLock: 0,
  synced: 0,
  deleted: 0,
  failed: 0,
  gaveUp: 0,
};

const selectReadyStmt = db.prepare(`
  SELECT note_id
  FROM notes
  WHERE status IN ('DIRTY', 'FAILED')
    AND stable_after <= @now
  ORDER BY stable_after ASC
  LIMIT 10
`);

const markSyncingStmt = db.prepare(`
  UPDATE notes
  SET status = 'SYNCING', locked_at = @locked_at, lock_owner = @lock_owner
  WHERE note_id = @note_id
    AND status IN ('DIRTY', 'FAILED')
    AND stable_after <= @now
`);

const getNoteStmt = db.prepare(`SELECT * FROM notes WHERE note_id = ?`);

const markSyncedStmt = db.prepare(`
  UPDATE notes
  SET
    status = 'SYNCED',
    notion_page_id = @notion_page_id,
    last_pushed_version = @last_pushed_version,
    last_pushed_at = @last_pushed_at,
    retry_count = 0,
    last_error = NULL,
    locked_at = NULL,
    lock_owner = NULL
  WHERE note_id = @note_id
`);

const markFailedStmt = db.prepare(`
  UPDATE notes
  SET
    status = @next_status,
    retry_count = retry_count + 1,
    stable_after = @stable_after,
    last_error = @last_error,
    locked_at = NULL,
    lock_owner = NULL
  WHERE note_id = @note_id
`);

const markDeletedStmt = db.prepare(`
  UPDATE notes
  SET
    status = 'DELETED',
    notion_page_id = @notion_page_id,
    last_pushed_version = @last_pushed_version,
    last_pushed_at = @last_pushed_at,
    locked_at = NULL,
    lock_owner = NULL
  WHERE note_id = @note_id
`);

async function dispatchOnce() {
  dispatcherMetrics.loops += 1;

  const now = new Date().toISOString();
  const rows = selectReadyStmt.all({ now });
  dispatcherMetrics.selected += rows.length;

  for (const row of rows) {
    const lockOwner = `pid-${process.pid}`;
    const lockResult = markSyncingStmt.run({
      note_id: row.note_id,
      locked_at: now,
      lock_owner: lockOwner,
      now,
    });

    if (lockResult.changes === 0) {
      dispatcherMetrics.skippedByLock += 1;
      continue;
    }

    const note = getNoteStmt.get(row.note_id);

    try {
      if (note.deleted) {
        const pageId = await notionUpsert(note);
        markDeletedStmt.run({
          note_id: note.note_id,
          notion_page_id: pageId,
          last_pushed_version: note.source_version,
          last_pushed_at: new Date().toISOString(),
        });
        dispatcherMetrics.deleted += 1;
        log.info(`marked deleted in notion: ${note.note_id}`);
        continue;
      }

      const pageId = await notionUpsert(note);

      markSyncedStmt.run({
        note_id: note.note_id,
        notion_page_id: pageId,
        last_pushed_version: note.source_version,
        last_pushed_at: new Date().toISOString(),
      });

      dispatcherMetrics.synced += 1;
      log.info(`synced to notion: ${note.note_id}`);
    } catch (err) {
      const nextRetryCount = Number(note.retry_count || 0) + 1;
      const isGiveUp = nextRetryCount >= config.maxRetryCount;
      const retryDelayMs = computeRetryDelayMs(
        nextRetryCount,
        config.retryBaseMs,
        config.retryMaxMs,
      );
      const nextStableAfter = isGiveUp
        ? new Date(Date.now() + 365 * 24 * 60 * 60 * 1000).toISOString()
        : new Date(Date.now() + retryDelayMs).toISOString();

      markFailedStmt.run({
        note_id: note.note_id,
        next_status: isGiveUp ? "GAVE_UP" : "FAILED",
        stable_after: nextStableAfter,
        last_error: err.message || String(err),
      });

      dispatcherMetrics.failed += 1;
      if (isGiveUp) {
        dispatcherMetrics.gaveUp += 1;
      }

      log.error(`sync failed: ${note.note_id}`, err.message || err);
      if (isGiveUp) {
        log.error(`sync gave up after max retries: ${note.note_id}`);
      }
    }
  }
}

let dispatchRunning = false;

function startDispatcher(intervalMs) {
  setInterval(() => {
    if (dispatchRunning) return;
    dispatchRunning = true;

    dispatchOnce()
      .catch((err) => {
        log.error("dispatcher loop failed:", err.message || err);
      })
      .finally(() => {
        dispatchRunning = false;
      });
  }, intervalMs);
}

function getDispatcherMetrics() {
  return { ...dispatcherMetrics };
}

module.exports = { startDispatcher, getDispatcherMetrics };
