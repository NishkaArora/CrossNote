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

  -- Answers from the demo page's "Does this note apply?" question. No IP
  -- address or other visitor identifier is stored.
  CREATE TABLE IF NOT EXISTS feedback (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    uri       TEXT NOT NULL,
    note_id   TEXT NOT NULL,
    answer    TEXT NOT NULL CHECK (answer IN ('yes', 'no', 'unsure')),
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

export function logFeedback(uri: string, noteId: string, answer: "yes" | "no" | "unsure", comment: string | null): void {
  db.prepare("INSERT INTO feedback (uri, note_id, answer, comment) VALUES (?, ?, ?, ?)")
    .run(uri, noteId, answer, comment);
}

export function removeLabel(uri: string, val: string): void {
  db.prepare("DELETE FROM labels WHERE uri = ? AND val = ?").run(uri, val);
}

// Post text is not stored: the `text` column is kept (empty) so databases
// created by earlier versions still work.
export function logLabel(
  uri: string,
  cid: string | undefined,
  val: string,
  comment: string | undefined
): void {
  db.prepare(
    "INSERT OR IGNORE INTO labels (uri, cid, val, text, comment) VALUES (?, ?, ?, '', ?)"
  ).run(uri, cid ?? null, val, comment ?? null);
}
