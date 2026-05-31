import Database from "better-sqlite3";

const DB_PATH = process.env.DATABASE_URL?.replace("file:", "") ?? "labeler.db";
const db = new Database(DB_PATH);

db.exec(`
  CREATE TABLE IF NOT EXISTS state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS labels (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    uri       TEXT NOT NULL UNIQUE,
    cid       TEXT,
    val       TEXT NOT NULL,
    text      TEXT NOT NULL,
    comment   TEXT,
    createdAt TEXT NOT NULL DEFAULT (datetime('now'))
  );
`);

// Migrate existing databases that predate the comment column
try {
  db.exec("ALTER TABLE labels ADD COLUMN comment TEXT");
} catch {
  // Column already exists — nothing to do
}

export function getCursor(): number | undefined {
  const row = db
    .prepare("SELECT value FROM state WHERE key = 'cursor'")
    .get() as { value: string } | undefined;
  return row ? parseInt(row.value, 10) : undefined;
}

export function setCursor(cursor: number): void {
  db.prepare(
    "INSERT OR REPLACE INTO state (key, value) VALUES ('cursor', ?)"
  ).run(String(cursor));
}

export function hasLabel(uri: string): boolean {
  const row = db.prepare("SELECT 1 FROM labels WHERE uri = ? LIMIT 1").get(uri);
  return row !== undefined;
}

export function logLabel(
  uri: string,
  cid: string | undefined,
  val: string,
  text: string,
  comment: string | undefined
): void {
  db.prepare(
    "INSERT OR IGNORE INTO labels (uri, cid, val, text, comment) VALUES (?, ?, ?, ?, ?)"
  ).run(uri, cid ?? null, val, text, comment ?? null);
}
