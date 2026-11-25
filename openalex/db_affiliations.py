"""Add the main affiliation of every author to the graph files.

usage: python db_affiliations.py [graph folder [authors.tab.gz]]      (defaults: graph authors.tab.gz)

2_authors.py extracts one affiliation per OpenAlex author record: of the institutions OpenAlex has seen on the
author's works, the one of the newest year (among several of that year the one with the most years). Several
records can carry one ORCID iD; the affiliation of the record with the most works counts (the later one of
equals), among the records that have one. The page shows it with the publications of an author.

An affiliation is what the works say, not what the person states: it can be out of date (the year tells), and
where OpenAlex has attached the works of a namesake it can be the namesake's.

Files, written into the graph folder next to the files of db_graph.py (numpy .npy, little endian). The server
reads them when they are there and fit the number of authors of the graph:
  institutions_blob.npy        uint8[B]     the distinct affiliations, each "name<TAB>country code" (UTF-8), concatenated
  institutions_off.npy         int64[K+1]   affiliation k = institutions_blob[off[k]:off[k+1]]
  author_institution.npy       int32[N]     affiliation of author i (index k), -1 = none known
  author_institution_year.npy  int16[N]     newest year in which a work of the author named it, 0 = unknown
"""
import csv
import gzip
import json
import os
import sys
import time

import numpy as np

graph_dir = sys.argv[1] if len(sys.argv) > 1 else "graph"
authors_path = sys.argv[2] if len(sys.argv) > 2 else "authors.tab.gz"
MAX_YEAR = time.localtime().tm_year + 1

t0 = time.time()
orcids = np.load(os.path.join(graph_dir, "orcids.npy"))
N = int(orcids.shape[0])
author_of = {o: i for i, o in enumerate(orcids.tolist())}      # ORCID iD (bytes, check digit X in upper case) -> author id
print(f"{N:,} authors from {graph_dir}", flush=True)

institution_of = {}                                   # "name\tcountry" -> index
author_institution = np.full(N, -1, dtype=np.int32)
author_year = np.zeros(N, dtype=np.int16)
works_of = np.full(N, -1, dtype=np.int64)             # works of the record whose affiliation is in place
n_rows = n_with = 0
csv.field_size_limit(1 << 30)
with gzip.open(authors_path, "rt", encoding="utf-8", newline="") as f:
    reader = csv.reader(f, delimiter="\t")
    header = next(reader)
    if "affiliation" not in header:
        raise SystemExit(f"{authors_path} has no affiliation column: extract it again with 2_authors.py")
    c_orcid, c_works = header.index("orcid"), header.index("works_count")
    c_name, c_country, c_year = header.index("affiliation"), header.index("affiliation_country"), header.index("affiliation_year")
    for row in reader:
        n_rows += 1
        if n_rows % 2_000_000 == 0:
            print(f"  {n_rows:,} author records ({time.time() - t0:.0f}s)", flush=True)
        if len(row) <= c_year or not row[c_name]:
            continue
        n_with += 1
        i = author_of.get(row[c_orcid].replace("https://orcid.org/", "").strip().upper().encode("utf-8")[:19])
        if i is None:
            continue
        works = int(row[c_works]) if row[c_works].isdigit() else 0
        if works < works_of[i]:
            continue
        works_of[i] = works
        key = row[c_name] + "\t" + row[c_country]
        k = institution_of.get(key)
        if k is None:
            k = institution_of[key] = len(institution_of)
        author_institution[i] = k
        year = int(row[c_year]) if row[c_year].isdigit() else 0
        author_year[i] = year if 0 < year <= MAX_YEAR else 0

names = [key.encode("utf-8") for key in institution_of]          # in index order
off = np.zeros(len(names) + 1, dtype=np.int64)
np.cumsum([len(b) for b in names], out=off[1:])
files = {"institutions_blob": np.frombuffer(b"".join(names), dtype=np.uint8), "institutions_off": off,
         "author_institution": author_institution, "author_institution_year": author_year}
for name, values in files.items():                               # written beside and moved into place: never half a file
    path = os.path.join(graph_dir, name + ".npy")
    np.save(path + ".tmp.npy", values)
    os.replace(path + ".tmp.npy", path)

n_authors = int((author_institution >= 0).sum())
meta_path = os.path.join(graph_dir, "meta.json")
meta = json.load(open(meta_path))
meta["affiliations"] = {"authors_with_one": n_authors, "institutions": len(names), "source": os.path.abspath(authors_path),
                        "built": time.strftime("%Y-%m-%d %H:%M:%S")}
json.dump(meta, open(meta_path + ".tmp", "w"), indent=1)
os.replace(meta_path + ".tmp", meta_path)
size = sum(os.path.getsize(os.path.join(graph_dir, name + ".npy")) for name in files)
print(f"Affiliations written to {graph_dir}: {n_authors:,} of {N:,} authors ({100 * n_authors / max(N, 1):.1f} %) at {len(names):,} institutions "
      f"({n_with:,} of {n_rows:,} author records carry one), {size / 2**20:.0f} MB, {time.time() - t0:.0f}s")
