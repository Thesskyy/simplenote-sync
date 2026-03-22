PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS notes (
  note_id TEXT PRIMARY KEY,
  content TEXT NOT NULL DEFAULT '',
  tags_json TEXT NOT NULL DEFAULT '[]',
  deleted INTEGER NOT NULL DEFAULT 0,

  source_version REAL,
  source_created_at TEXT,
  source_updated_at TEXT,
  last_event_at TEXT NOT NULL,
  stable_after TEXT NOT NULL,

  status TEXT NOT NULL DEFAULT 'DIRTY',  -- DIRTY / SYNCING / SYNCED / FAILED / GAVE_UP / DELETED
  notion_page_id TEXT,
  last_pushed_version REAL,
  last_pushed_at TEXT,
  retry_count INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,

  locked_at TEXT,
  lock_owner TEXT
);

CREATE TABLE IF NOT EXISTS note_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  note_id TEXT NOT NULL,
  source_version REAL,
  event_type TEXT NOT NULL,              -- update / delete
  received_at TEXT NOT NULL,
  raw_note_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_notes_dispatch
ON notes(status, stable_after);

CREATE INDEX IF NOT EXISTS idx_note_events_note_id
ON note_events(note_id);

CREATE INDEX IF NOT EXISTS idx_note_events_received_at
ON note_events(received_at);