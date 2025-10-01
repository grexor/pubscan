import sqlite3
import os

AUTHOR_FILE = "/home/gregor/pubscan/parser/author_names.tab"
DB_FILE = "/home/gregor/pubscan/parser/authors.db"

# Remove old DB if it exists (optional)
if os.path.exists(DB_FILE):
    os.remove(DB_FILE)

# Connect
conn = sqlite3.connect(DB_FILE)
c = conn.cursor()

# Create FTS5 table with prefix indexing
# 'prefix="2 3 4"' indexes 2-, 3-, and 4-char prefixes for each token
c.execute("CREATE VIRTUAL TABLE authors USING fts5(name, prefix='2 3 4 5 6 7 8 9 10')")

# Load authors file
with open(AUTHOR_FILE, "r", encoding="utf-8") as f:
    rows = [(line.strip(),) for line in f if line.strip()]

# Insert into table
c.executemany("INSERT INTO authors(name) VALUES (?)", rows)
conn.commit()
conn.close()

print(f"FTS5 database built: {DB_FILE} with {len(rows)} authors")

