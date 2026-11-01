"""Build the in-memory co-authorship graph files from pubscan.db.

usage: python db_graph.py <pubscan.db> <output folder>      (default: pubscan.db graph)

The server (pubscan/graph.py) memory-maps these files instead of parsing the comma-separated
PMID lists in SQLite on every request. Everything is derived from the served database, so the
network output is identical to the SQLite path.

Files (numpy .npy, little endian):
  orcids.npy            S19[N]        ORCID strings exactly as stored in authors.orcid, sorted; author id = index
  names_blob.npy        uint8[B]      UTF-8 author names, concatenated in author id order
  names_off.npy         int64[N+1]    name of author i = names_blob[off[i]:off[i+1]]
  author_pmids.npy      int32[P]      PMIDs per author, newest first (same order as index.py's author_pmids)
  author_pmids_off.npy  int64[N+1]    PMIDs of author i = author_pmids[off[i]:off[i+1]]
  pmid_authors.npy      int32[Q]      author ids per PMID, in the paper's author order (publications.authors_orcid)
  pmid_authors_off.npy  int64[M+2]    authors of PMID p = pmid_authors[off[p]:off[p+1]]  (M = max PMID)
  pmid_year.npy         int16[M+1]    publication year, 0 = row without a usable year, -1 = no publications row
  meta.json             counts, source file and build time
"""
import sys
import os
import json
import time
import sqlite3
import array
import numpy as np

db_path = sys.argv[1] if len(sys.argv) > 1 else "pubscan.db"
out_dir = sys.argv[2] if len(sys.argv) > 2 else "graph"
os.makedirs(out_dir, exist_ok=True)
t0 = time.time()

conn = sqlite3.connect(f"file:{db_path}?immutable=1", uri=True)
conn.execute("PRAGMA cache_size = -1000000;")

# ---------------------------------------------------------------- pass 1: authors
print("Reading authors ...", flush=True)
orcids, names, pmid_lists = [], [], []
cur = conn.execute("SELECT orcid, author_name, pmids FROM authors ORDER BY rowid")
for i, (orcid, name, pmids) in enumerate(cur, 1):
    orcids.append(orcid)
    names.append(name or "")
    if pmids:
        arr = np.fromstring(pmids, dtype=np.int64, sep=",")
        arr = np.unique(arr)[::-1].astype(np.int32)       # unique, descending = newest first
    else:
        arr = np.zeros(0, dtype=np.int32)
    pmid_lists.append(arr)
    if i % 1_000_000 == 0:
        print(f"  {i:,} authors", flush=True)
N = len(orcids)
print(f"  {N:,} authors read in {time.time() - t0:.0f}s", flush=True)

# author ids in sorted ORCID order, so that lookups are a binary search on the ORCID table
orcid_arr = np.array(orcids, dtype="S19")
order = np.argsort(orcid_arr, kind="stable")
orcid_sorted = orcid_arr[order]
if N > 1 and np.any(orcid_sorted[1:] == orcid_sorted[:-1]):
    raise SystemExit("duplicate ORCIDs in the authors table (primary key violated?)")
np.save(os.path.join(out_dir, "orcids.npy"), orcid_sorted)

name_bytes = [names[j].encode("utf-8") for j in order]
names_off = np.zeros(N + 1, dtype=np.int64)
names_off[1:] = np.cumsum([len(b) for b in name_bytes])
np.save(os.path.join(out_dir, "names_blob.npy"), np.frombuffer(b"".join(name_bytes), dtype=np.uint8))
np.save(os.path.join(out_dir, "names_off.npy"), names_off)
del name_bytes, names

lengths = np.array([len(pmid_lists[j]) for j in order], dtype=np.int64)
author_pmids_off = np.zeros(N + 1, dtype=np.int64)
author_pmids_off[1:] = np.cumsum(lengths)
author_pmids = np.concatenate([pmid_lists[j] for j in order]) if N else np.zeros(0, np.int32)
np.save(os.path.join(out_dir, "author_pmids.npy"), author_pmids)
np.save(os.path.join(out_dir, "author_pmids_off.npy"), author_pmids_off)
P = int(author_pmids.shape[0])
print(f"  {P:,} author-PMID pairs written in {time.time() - t0:.0f}s", flush=True)
del pmid_lists, author_pmids

# orcid -> id for pass 2
orcid_to_id = {o: i for i, o in enumerate(orcid_sorted.tolist())}   # bytes keys
del orcid_arr, order

# ---------------------------------------------------------------- pass 2: publications
print("Reading publications ...", flush=True)
M = conn.execute("SELECT max(pmid) FROM publications").fetchone()[0] or 0
counts = np.zeros(M + 1, dtype=np.int32)
pmid_year = np.full(M + 1, -1, dtype=np.int16)      # -1: no publications row
pmid_authors = array.array("i")
unknown_authors = 0
cur = conn.execute("SELECT pmid, pub_year, authors_orcid FROM publications ORDER BY pmid")
for i, (pmid, year, authors_orcid) in enumerate(cur, 1):
    try:
        y = int(year)
        y = y if 0 < y < 32767 else 0
    except (TypeError, ValueError):
        y = 0
    pmid_year[pmid] = y
    k = 0
    for o in (authors_orcid or "").split(","):
        o = o.strip()
        if not o:
            continue
        aid = orcid_to_id.get(o.encode("utf-8")[:19])
        if aid is None:
            unknown_authors += 1
            continue
        pmid_authors.append(aid)
        k += 1
    counts[pmid] = k
    if i % 5_000_000 == 0:
        print(f"  {i:,} publications", flush=True)
pmid_authors_off = np.zeros(M + 2, dtype=np.int64)
pmid_authors_off[1:] = np.cumsum(counts, dtype=np.int64)
pmid_authors = np.frombuffer(pmid_authors, dtype=np.int32)
assert pmid_authors.shape[0] == pmid_authors_off[-1]
np.save(os.path.join(out_dir, "pmid_authors.npy"), pmid_authors)
np.save(os.path.join(out_dir, "pmid_authors_off.npy"), pmid_authors_off)
np.save(os.path.join(out_dir, "pmid_year.npy"), pmid_year)
Q = int(pmid_authors.shape[0])
print(f"  {i:,} publications, {Q:,} PMID-author pairs ({unknown_authors:,} author entries not in the authors table) in {time.time() - t0:.0f}s", flush=True)

meta = {"authors": N, "author_pmid_pairs": P, "publications": int(i), "pmid_author_pairs": Q, "max_pmid": int(M),
        "source": os.path.abspath(db_path), "source_mtime": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(os.path.getmtime(db_path))),
        "built": time.strftime("%Y-%m-%d %H:%M:%S"), "format": 1}
json.dump(meta, open(os.path.join(out_dir, "meta.json"), "w"), indent=1)
total = sum(os.path.getsize(os.path.join(out_dir, f)) for f in os.listdir(out_dir))
print(f"Graph built in {out_dir}: {total / 2**30:.2f} GB, {time.time() - t0:.0f}s")
