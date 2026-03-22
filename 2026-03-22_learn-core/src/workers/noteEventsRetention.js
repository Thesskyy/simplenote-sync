const db = require("../db");
const config = require("../config");
const log = require("../logger");

const purgeOldNoteEventsStmt = db.prepare(`
  DELETE FROM note_events
  WHERE received_at < @cutoff
`);

function startNoteEventsRetention() {
  if (
    !Number.isFinite(config.noteEventsRetentionDays) ||
    config.noteEventsRetentionDays <= 0
  ) {
    log.info("note_events retention disabled");
    return;
  }

  const runOnce = () => {
    const cutoffMs =
      Date.now() - config.noteEventsRetentionDays * 24 * 60 * 60 * 1000;
    const cutoff = new Date(cutoffMs).toISOString();
    const result = purgeOldNoteEventsStmt.run({ cutoff });

    if (result.changes > 0) {
      log.info("purged note_events", {
        deletedRows: result.changes,
        cutoff,
      });
    }
  };

  runOnce();
  setInterval(runOnce, config.noteEventsPurgeIntervalMs);
}

module.exports = { startNoteEventsRetention };
