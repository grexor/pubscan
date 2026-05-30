import sqlite3
import csv
import gzip
from pathlib import Path
import time
from unidecode import unidecode

# === Config ===
BASE_DIR = Path("/home/gregor/pubscan3")
DB_FILE = BASE_DIR / "parser" / "pubscan.db"

PUBLICATIONS_FILE = BASE_DIR / "openalex" / "publications_with_orcids.tsv.gz"

# === Helper ===
def to_ascii(text):
    if not text:
        return ''
    return unidecode(text).strip()

# === PRAGMAs ===
PRAGMAS_FAST = """
PRAGMA synchronous = OFF;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = 2000000;
PRAGMA mmap_size = 30000000000;
"""

PRAGMAS_READONLY = """
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = 1000000;
"""

# === Release any lingering locks ===
conn = sqlite3.connect(DB_FILE)
conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
conn.commit()
conn.close()

# === Build main PUBSCAN DB ===
t0 = time.time()
conn = sqlite3.connect(DB_FILE)
c = conn.cursor()
c.executescript(PRAGMAS_FAST)

# === Schema ===
c.executescript("""
DROP TABLE IF EXISTS publications;
CREATE TABLE publications (
    pmid          INTEGER NOT NULL PRIMARY KEY,
    title         TEXT NOT NULL,
    pub_year      INTEGER,
    authors_all   TEXT,
    authors_name  TEXT,
    authors_orcid TEXT
);
""")

# === Import publications ===
print("Importing publications...")
with gzip.open(PUBLICATIONS_FILE, "rt", encoding="utf-8", errors="replace") as f:
    reader = csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
    next(reader)  # skip header
    batch = []
    commit_every = 100_000
    for i, row in enumerate(reader, 1):
        if len(row) != 9:
            continue
        try:
            row = [
                int(row[1]),                             # pmid
                to_ascii(row[2]),                        # title
                row[5],                                  # pub_year
                to_ascii(row[8]).replace(";", ","),      # authors_all
                to_ascii(row[7]).replace(";", ","),      # authors_name
                row[6].replace(";", ","),                # authors_orcid
            ]
        except Exception:
            continue
        batch.append(tuple(row))
        if len(batch) >= 5_000:
            c.executemany(
                "INSERT OR IGNORE INTO publications (pmid, title, pub_year, authors_all, authors_name, authors_orcid) VALUES (?, ?, ?, ?, ?, ?)",
                batch,
            )
            batch.clear()
        if i % commit_every == 0:
            conn.commit()
            print(f"  Publications: {i:,} imported...")
    if batch:
        c.executemany(
            "INSERT OR IGNORE INTO publications (pmid, title, pub_year, authors_all, authors_name, authors_orcid) VALUES (?, ?, ?, ?, ?, ?)",
            batch,
        )
conn.commit()

# === Indexes ===
print("Creating indexes...")
c.executescript("""
CREATE INDEX IF NOT EXISTS idx_publications_year ON publications(pub_year);
""")

c.execute("PRAGMA optimize;")
conn.commit()
conn.executescript(PRAGMAS_READONLY)
conn.close()

print(f"Main DB built: {DB_FILE} in {time.time() - t0:.1f}s")