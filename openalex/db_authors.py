import sqlite3
import csv
import gzip
from pathlib import Path
import time
from unidecode import unidecode

DB_FILE = "pubscan.db"
DB_FILE_NAMES = "names.db"
AUTHORS_FILE = "authors_publications.tab.gz"

# === Helper ===
def to_ascii(text):
    if not text:
        return ''
    return unidecode(text).strip()

# === PRAGMAs ===
PRAGMAS_FAST = """
PRAGMA journal_mode = MEMORY;
PRAGMA synchronous = OFF;
PRAGMA locking_mode = EXCLUSIVE;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = 2000000;
PRAGMA mmap_size = 30000000000;
"""

PRAGMAS_READONLY = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = 1000000;
"""

# === Build main PUBSCAN DB ===
t0 = time.time()
conn = sqlite3.connect(DB_FILE)
c = conn.cursor()
c.execute("PRAGMA wal_checkpoint(TRUNCATE);")
c.executescript(PRAGMAS_FAST)

# === Schema ===
c.executescript("""
DROP TABLE IF EXISTS authors;
CREATE TABLE IF NOT EXISTS authors (
    orcid       TEXT NOT NULL PRIMARY KEY,
    author_name TEXT NOT NULL,
    pmids       TEXT
);
""")

# === Import authors ===
print("Importing authors...")
with gzip.open(AUTHORS_FILE, "rt", encoding="utf-8", errors="replace") as f:
    reader = csv.reader(f, delimiter="\t")
    next(reader)  # skip header
    batch = []
    commit_every = 200_000
    for i, row in enumerate(reader, 1):
        if len(row) != 2:
            continue
        parts = row[0].split(" ")
        orcid = parts[-1].strip()
        name  = to_ascii(" ".join(parts[:-1]))
        pmids = row[1].strip()
        if not orcid or not name:
            continue
        batch.append((orcid, name, pmids))
        if len(batch) >= 10_000:
            c.executemany(
                "INSERT OR IGNORE INTO authors (orcid, author_name, pmids) VALUES (?, ?, ?)",
                batch,
            )
            batch.clear()
        if i % commit_every == 0:
            conn.commit()
            print(f"  Authors: {i:,} imported...")
    if batch:
        c.executemany(
            "INSERT OR IGNORE INTO authors (orcid, author_name, pmids) VALUES (?, ?, ?)",
            batch,
        )
conn.commit()

# === Indexes ===
print("Creating indexes...")
c.executescript("""
CREATE INDEX IF NOT EXISTS idx_authors_name ON authors(author_name);
""")

c.execute("PRAGMA optimize;")
conn.commit()
conn.executescript(PRAGMAS_READONLY)
conn.close()

print(f"Main DB built: {DB_FILE} in {time.time() - t0:.1f}s")