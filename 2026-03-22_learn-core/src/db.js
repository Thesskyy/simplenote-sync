const fs = require("fs");
const path = require("path");
const Database = require("better-sqlite3");

const dbPath = path.resolve(__dirname, "..", "data", "app.db");
const sqlPath = path.resolve(__dirname, "..", "sql", "001_init.sql");

fs.mkdirSync(path.dirname(dbPath), { recursive: true });

const db = new Database(dbPath);
db.pragma("journal_mode = WAL");
db.pragma("synchronous = NORMAL");
db.pragma("busy_timeout = 5000");

const initSql = fs.readFileSync(sqlPath, "utf8");
db.exec(initSql);

function ensureColumn(tableName, columnName, alterSql) {
  const columns = db.prepare(`PRAGMA table_info(${tableName})`).all();
  const hasColumn = columns.some((c) => c.name === columnName);
  if (!hasColumn) {
    db.exec(alterSql);
  }
}

ensureColumn(
  "notes",
  "source_created_at",
  "ALTER TABLE notes ADD COLUMN source_created_at TEXT",
);

module.exports = db;
