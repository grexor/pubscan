import sqlite3
import csv
import gzip
from pathlib import Path
import time
from unidecode import unidecode

# === Config ===
BASE_DIR = Path("/home/gregor/pubscan3")
DB_FILE = BASE_DIR / "parser" / "pubscan.db"
DB_FILE_NAMES = BASE_DIR / "parser" / "names.db"

AUTHORS_FILE = BASE_DIR / "openalex" / "authors_final.tsv.gz"
PUBLICATIONS_FILE = BASE_DIR / "openalex" / "publications_with_orcids.tsv.gz"

# === Helper ===
def fresh_db(path):
    if path.exists():
        path.unlink()

def to_ascii(text):
    if not text:
        return ''
    return unidecode(text).strip()

# === Remove old DBs ===
fresh_db(DB_FILE)
fresh_db(DB_FILE_NAMES)

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

# === Build NAMES FTS database ===
t0 = time.time()
conn_names = sqlite3.connect(DB_FILE_NAMES)
c_names = conn_names.cursor()
c_names.executescript(PRAGMAS_FAST)

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
    next(reader)  # skip header
    batch = []
    commit_every = 200_000
    for i, row in enumerate(reader, 1):
        if len(row) != 2:
            continue
        #name = to_ascii(" ".join(row[0].split(" ")[:-1]))  # strip orcid from end
        name = to_ascii(row[0])
        if not name:
            continue
        batch.append((name,))
        if len(batch) >= 10_000:
            c_names.executemany("INSERT INTO names(name) VALUES (?)", batch)
            batch.clear()
        if i % commit_every == 0:
            conn_names.commit()
            print(f"  FTS: {i:,} authors indexed...")
    if batch:
        c_names.executemany("INSERT INTO names(name) VALUES (?)", batch)
conn_names.commit()
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
