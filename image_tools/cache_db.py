"""類似画像キャッシュ用 SQLite（`similar_images` と JSON→DB 変換で共有）。"""

import sqlite3

DELETE_DB_CHUNK = 512

SQL_INSERT_OR_REPLACE_IMAGE = (
    "INSERT OR REPLACE INTO images VALUES (?,?,?,?,?,?,?,?)"
)


def init_db(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-200000")
    conn.execute("PRAGMA temp_store=MEMORY")
    c = conn.cursor()
    c.execute(
        """CREATE TABLE IF NOT EXISTS images
                 (path TEXT PRIMARY KEY, hash_str TEXT, color_hash_str TEXT, pixels INTEGER,
                  filesize INTEGER, aspect_ratio REAL, mtime REAL, checked INTEGER DEFAULT 0)"""
    )
    try:
        c.execute("ALTER TABLE images ADD COLUMN checked INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    c.execute(
        """CREATE TABLE IF NOT EXISTS similarity_edges (path1 TEXT, path2 TEXT, PRIMARY KEY(path1, path2))"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT)"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS folder_mtimes (path TEXT PRIMARY KEY, mtime REAL)"""
    )
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_exact ON images(hash_str, color_hash_str, pixels, filesize)"
    )
    conn.commit()
    return conn, c


def delete_db_records(c, paths):
    for i in range(0, len(paths), DELETE_DB_CHUNK):
        chunk = paths[i : i + DELETE_DB_CHUNK]
        c.executemany("DELETE FROM images WHERE path = ?", [(p,) for p in chunk])
        c.executemany(
            "DELETE FROM similarity_edges WHERE path1 = ? OR path2 = ?",
            [(p, p) for p in chunk],
        )
