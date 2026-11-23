"""Build the full-text index of the author names for the search box.

usage: python db_names.py <graph folder> <names.db> [<authors.tab.gz> [<orcid_records.tab.gz>]]
       (defaults: graph names.db authors.tab.gz orcid_records.tab.gz)

One row per author of the graph, in an SQLite FTS5 table with two columns:
  name   "Name ORCID", the name as the graph stores it (the name stated in the ORCID record, transliterated to
         ASCII); this is what the search box shows
  alt    words of the other spellings of the author that the name itself lacks: the names of the ORCID record
         (given and family name, credit name, other names) and the name and name alternatives of OpenAlex ("a"
         for Andrej Seljak, whom OpenAlex calls "A. Seljak"). Searched, never shown.

The table has prefix indexes for 2 to 6 characters, which is what the autocomplete of the search box queries.
A query returns its first 50 matches in the order of insertion, so the authors go in with the most publications
first: for a common name the suggestions are then drawn from the most published bearers of that name.
"""
import csv
import gzip
import os
import re
import sqlite3
import sys
import time

import numpy as np
from unidecode import unidecode

graph_dir = sys.argv[1] if len(sys.argv) > 1 else "graph"
db_file = sys.argv[2] if len(sys.argv) > 2 else "names.db"
authors_path = sys.argv[3] if len(sys.argv) > 3 else "authors.tab.gz"
orcid_path = sys.argv[4] if len(sys.argv) > 4 else "orcid_records.tab.gz"
MAX_WORDS = 24                    # words of other spellings kept per author
WORD = re.compile(r"[a-z0-9]+")

t0 = time.time()
orcids = np.load(os.path.join(graph_dir, "orcids.npy"))
names_blob = np.load(os.path.join(graph_dir, "names_blob.npy")).tobytes()
names_off = np.load(os.path.join(graph_dir, "names_off.npy"))
n_pubs = np.diff(np.load(os.path.join(graph_dir, "author_pubs_off.npy")))
N = int(orcids.shape[0])
order = np.lexsort((np.arange(N), -n_pubs))            # most publications first, then by ORCID
print(f"{N:,} authors from {graph_dir}", flush=True)

# other spellings: ORCID -> words (lowercase ASCII, in order of first appearance); several records can carry one ORCID
in_graph = set(orcids.tolist())
other_words = {}


def add_words(orcid, text):
    """Remember the words of one more spelling of the author (initials included: "g" lets "rot g" find Gregor Rot)."""
    words = other_words.get(orcid, "").split()
    known = set(words)
    for word in WORD.findall(unidecode(text).lower()):
        if word not in known:
            known.add(word)
            words.append(word)
    other_words[orcid] = " ".join(words)


if os.path.exists(orcid_path):                        # credit name and other names of the ORCID record
    with gzip.open(orcid_path, "rt", encoding="utf-8", newline="\n") as f:
        next(f)                                       # header: orcid, given, family, credit, other, works
        for line in f:
            cells = line.split("\t", 5)
            if len(cells) < 6:
                continue
            orcid = cells[0].upper().encode("utf-8")
            if orcid in in_graph:                     # given and family name too: they are the shown name only when the
                add_words(orcid, " ".join(cells[1:5]))   # graph was built with the names of the ORCID records
    print(f"  names of the ORCID records of {len(other_words):,} authors ({time.time() - t0:.0f}s)", flush=True)
else:
    print(f"  {orcid_path} not found: no credit names and other names of ORCID records", flush=True)
if os.path.exists(authors_path):                      # name and name alternatives of OpenAlex
    csv.field_size_limit(1 << 30)
    with gzip.open(authors_path, "rt", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter="\t")
        next(reader)                                  # header: openalex_id, name, orcid, alternatives, works_count
        for row in reader:
            if len(row) < 3:
                continue
            orcid = row[2].replace("https://orcid.org/", "").strip().upper().encode("utf-8")[:19]
            if orcid in in_graph:
                add_words(orcid, row[1] + " " + (row[3] if len(row) > 3 else ""))
    print(f"  with the spellings of OpenAlex: {len(other_words):,} authors ({time.time() - t0:.0f}s)", flush=True)
else:
    print(f"  {authors_path} not found: no spellings of OpenAlex", flush=True)
del in_graph

if os.path.exists(db_file):
    os.remove(db_file)
conn = sqlite3.connect(db_file)
c = conn.cursor()
c.executescript("""
PRAGMA synchronous = OFF;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = 2000000;
CREATE VIRTUAL TABLE names USING fts5(
    name,
    alt,
    prefix='2 3 4 5 6',
    tokenize='unicode61'
);
""")

batch = []
n_with_alt = 0
for done, i in enumerate(order.tolist(), 1):
    name = names_blob[names_off[i]:names_off[i + 1]].decode("utf-8", "replace")
    orcid = orcids[i]
    alt = ""
    words = other_words.get(orcid)
    if words:
        shown = set(WORD.findall(name.lower()))
        alt = " ".join([w for w in words.split() if w not in shown][:MAX_WORDS])
        n_with_alt += bool(alt)
    batch.append((f"{name} {orcid.decode('utf-8')}", alt))
    if len(batch) >= 10_000:
        c.executemany("INSERT INTO names(name, alt) VALUES (?, ?)", batch)
        batch.clear()
    if done % 1_000_000 == 0:
        conn.commit()
        print(f"  {done:,} authors indexed ({time.time() - t0:.0f}s)", flush=True)
if batch:
    c.executemany("INSERT INTO names(name, alt) VALUES (?, ?)", batch)
conn.commit()
c.execute("INSERT INTO names(names) VALUES('optimize');")
conn.commit()
conn.executescript("PRAGMA synchronous = NORMAL;")
conn.close()
print(f"Name index built: {db_file}, {os.path.getsize(db_file) / 2**30:.2f} GB, {n_with_alt:,} authors with words from other spellings, {time.time() - t0:.0f}s")
