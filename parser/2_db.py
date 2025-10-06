import sqlite3
import csv
import gzip
import os
from pathlib import Path
import time

# === Config ===
BASE_DIR = Path("/home/gregor/pubscan/parser")
DB_FILE = BASE_DIR / "pubscan.db"
DB_FILE_NAMES = BASE_DIR / "names.db"

AUTHORS_FILE = BASE_DIR / "authors.tab.gz"
PUBLICATIONS_FILE = BASE_DIR / "publications.tab.gz"

# === Helper ===
def fresh_db(path):
    if path.exists():
        path.unlink()

# === Remove old DBs ===
fresh_db(DB_FILE)
fresh_db(DB_FILE_NAMES)

# === PRAGMAs (performance tuning) ===
PRAGMAS_FAST = """
PRAGMA journal_mode = MEMORY;
PRAGMA synchronous = OFF;
PRAGMA locking_mode = EXCLUSIVE;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = 2000000;
PRAGMA mmap_size = 30000000000;  -- 30 GB (use available RAM)
"""

PRAGMAS_READONLY = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = 1000000;
"""

# === Build NAMES FTS database ===
t0 = time.time()
conn_names = sqlite3.connect(DB_FILE_NAMES)
c_names = conn_names.cursor()
c_names.executescript(PRAGMAS_FAST)

# Use better tokenizer and smaller prefix set for speed
c_names.executescript("""
CREATE VIRTUAL TABLE names USING fts5(
    name,
    prefix='2 3 4 5 6',
    tokenize='unicode61'
);
""")

print("Importing author_names...")
with gzip.open(AUTHORS_FILE, "rt", encoding="utf-8", errors="replace") as f:
    reader = csv.reader(f, delimiter="\t")
    batch = []
    commit_every = 200_000
    for i, row in enumerate(reader, 1):
        if len(row) != 2:
            continue
        name = row[0].strip()
        batch.append((name,))
        if len(batch) >= 10_000:
            c_names.executemany("INSERT INTO names(name) VALUES (?)", batch)
            batch.clear()
        if i % commit_every == 0:
            conn_names.commit()
    if batch:
        c_names.executemany("INSERT INTO names(name) VALUES (?)", batch)
conn_names.commit()

# Optimize FTS index structure
c_names.execute("INSERT INTO names(names) VALUES('optimize');")
conn_names.commit()
conn_names.executescript(PRAGMAS_READONLY)
conn_names.close()
print(f"FTS database built: {DB_FILE_NAMES} in {time.time() - t0:.1f}s")

# === Build main PUBSCAN DB ===
t0 = time.time()
conn = sqlite3.connect(DB_FILE)
c = conn.cursor()
c.executescript(PRAGMAS_FAST)

# === Schema ===
c.executescript("""
CREATE TABLE authors (
    author_name TEXT NOT NULL PRIMARY KEY,
    pmids TEXT
);

CREATE TABLE publications (
    pmid INTEGER NOT NULL PRIMARY KEY,
    title TEXT NOT NULL,
    pub_year INTEGER,
    authors TEXT
);
""")

# === Import authors ===
print("Importing authors...")
with gzip.open(AUTHORS_FILE, "rt", encoding="utf-8", errors="replace") as f:
    reader = csv.reader(f, delimiter="\t")
    batch = []
    commit_every = 200_000
    for i, row in enumerate(reader, 1):
        if len(row) != 2:
            continue
        batch.append((row[0].strip(), row[1].strip()))
        if len(batch) >= 10_000:
            c.executemany(
                "INSERT OR IGNORE INTO authors (author_name, pmids) VALUES (?, ?)",
                batch,
            )
            batch.clear()
        if i % commit_every == 0:
            conn.commit()
    if batch:
        c.executemany(
            "INSERT OR IGNORE INTO authors (author_name, pmids) VALUES (?, ?)",
            batch,
        )
conn.commit()

# === Import publications ===
print("Importing publications...")
with gzip.open(PUBLICATIONS_FILE, "rt", encoding="utf-8", errors="replace") as f:
    reader = csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
    batch = []
    commit_every = 100_000
    for i, row in enumerate(reader, 1):
        row = (row + [None] * 4)[:4]  # pad/truncate to 4 cols
        batch.append(tuple(row))
        if len(batch) >= 5_000:
            c.executemany(
                "INSERT OR IGNORE INTO publications (pmid, title, pub_year, authors) VALUES (?, ?, ?, ?)",
                batch,
            )
            batch.clear()
        if i % commit_every == 0:
            conn.commit()
    if batch:
        c.executemany(
            "INSERT OR IGNORE INTO publications (pmid, title, pub_year, authors) VALUES (?, ?, ?, ?)",
            batch,
        )
conn.commit()

# === Indexes ===
print("Creating indexes...")
c.executescript("""
CREATE INDEX IF NOT EXISTS idx_authors_name ON authors(author_name);
CREATE INDEX IF NOT EXISTS idx_publications_year ON publications(pub_year);
""")

# Final optimize and WAL switch for stable read-only mode
c.execute("PRAGMA optimize;")
conn.commit()
conn.executescript(PRAGMAS_READONLY)
conn.close()

print(f"Main DB built: {DB_FILE} in {time.time() - t0:.1f}s")
