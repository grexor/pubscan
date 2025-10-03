import sqlite3
import csv
import gzip
import os
from pathlib import Path

# === Config ===
BASE_DIR = Path("/home/gregor/pubscan/parser")
DB_FILE = BASE_DIR / "pubscan.db"

AUTHORS_FILE = BASE_DIR / "authors.tab.gz"
PUBLICATIONS_FILE = BASE_DIR / "publications.tab.gz"

# === Remove old DB ===
if DB_FILE.exists():
    DB_FILE.unlink()

# === Connect ===
conn = sqlite3.connect(DB_FILE)
c = conn.cursor()

# Speed optimizations
c.executescript("""
PRAGMA journal_mode = WAL;
PRAGMA synchronous = OFF;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = 1000000;
""")

# === Create tables ===
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

CREATE VIRTUAL TABLE names USING fts5(
    name, 
    prefix='2 3 4 5 6 7 8 9 10'
);
""")

print("Importing author_names...")
with gzip.open(AUTHORS_FILE, "rt", encoding="utf-8", errors="replace") as f:
    reader = csv.reader(f, delimiter='\t')
    batch = []
    for row in reader:
        if len(row) != 2:
            continue
        name = row[0].strip()
        batch.append((name,))
        if len(batch) >= 10000:
            c.executemany("INSERT INTO names(name) VALUES (?)", batch)
            batch.clear()
    if batch:
        c.executemany("INSERT INTO names(name) VALUES (?)", batch)

print("Importing authors...")
with gzip.open(AUTHORS_FILE, "rt", encoding="utf-8", errors="replace") as f:
    reader = csv.reader(f, delimiter='\t')
    batch = []
    for row in reader:
        if len(row) != 2:
            continue
        batch.append(row)
        if len(batch) >= 10000:
            c.executemany("INSERT OR IGNORE INTO authors (author_name, pmids) VALUES (?, ?)", batch)
            batch.clear()
    if batch:
        c.executemany("INSERT OR IGNORE INTO authors (author_name, pmids) VALUES (?, ?)", batch)

print("Importing publications...")
with gzip.open(PUBLICATIONS_FILE, "rt", encoding="utf-8", errors="replace") as f:
    reader = csv.reader(f, delimiter='\t', quoting=csv.QUOTE_NONE)
    batch = []
    for row in reader:
        if len(row) < 4:
            row += [None] * (4 - len(row))  # pad missing columns
        elif len(row) > 4:
            row = row[:4]
        batch.append(row)
        if len(batch) >= 5000:
            c.executemany("INSERT OR IGNORE INTO publications (pmid, title, pub_year, authors) VALUES (?, ?, ?, ?)", batch)
            batch.clear()
    if batch:
        c.executemany("INSERT OR IGNORE INTO publications (pmid, title, pub_year, authors) VALUES (?, ?, ?, ?)", batch)

# === Indexes ===
c.executescript("""
CREATE INDEX IF NOT EXISTS idx_authors_name ON authors(author_name);
CREATE INDEX IF NOT EXISTS idx_publications_year ON publications(pub_year);
""")

conn.commit()
conn.close()

print(f"Database built: {DB_FILE}")
