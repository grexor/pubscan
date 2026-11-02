"""Build the author-topic table from topics.tab.gz (written by 3_publications.py).

usage: python db_topics.py <topics.tab.gz> <topics_vocab.tab.gz> <graph dir> <output folder>
       (defaults: topics.tab.gz topics_vocab.tab.gz graph topics)

Every PubMed work in OpenAlex carries up to three topics from a fixed taxonomy (each with a subfield,
field and domain) and about ten scored keywords. This script counts, for every author and every term,
the number of the author's publications tagged with that term, and stores the result term by term so
that "who publishes on X" is one slice of an array. Terms are the topics, their subfields and the
keywords with a score of at least KEYWORD_MIN_SCORE. Authors are identified by their id in the
memory-mapped graph (position in graph/orcids.npy), so the server can take names from the graph.

Files (numpy .npy, little endian) in the output folder:
  term_off.npy      int64[T+1]   authors of term t = slice off[t]:off[t+1] of the three arrays below
  term_authors.npy  int32[P]     graph author ids, ordered by publication count (descending) within a term
  term_counts.npy   uint16[P]    the author's number of publications carrying the term
  term_years.npy    int16[P]     year of the author's newest publication carrying the term (0 = unknown)
  terms.json        list of T terms: {"name", "kind": "topic"|"subfield"|"keyword", "id", "subfield", "field",
                                      "domain", "n_authors", "n_pubs"}; the list index is the term id
  meta.json         counts, source files and build time
"""
import sys
import os
import csv
import gzip
import json
import time
import array
import numpy as np

topics_path = sys.argv[1] if len(sys.argv) > 1 else "topics.tab.gz"
vocab_path = sys.argv[2] if len(sys.argv) > 2 else "topics_vocab.tab.gz"
graph_dir = sys.argv[3] if len(sys.argv) > 3 else "graph"
out_dir = sys.argv[4] if len(sys.argv) > 4 else "topics"
KEYWORD_MIN_SCORE = float(os.environ.get("KEYWORD_MIN_SCORE", "0.5"))
N_PARTITIONS = 16                 # pair keys are spilled to disk by author id range and aggregated per partition
TERM_BITS = 24                    # pair key = author id << TERM_BITS | term id
FLUSH = 20_000_000                # keys buffered per partition before writing

os.makedirs(out_dir, exist_ok=True)
tmp_dir = os.path.join(out_dir, "tmp_pairs")
os.makedirs(tmp_dir, exist_ok=True)
t0 = time.time()

# ------------------------------------------------------------------ authors: ORCID -> graph id
orcids = np.load(os.path.join(graph_dir, "orcids.npy"))
N = int(orcids.shape[0])
orcid_to_id = {o: i for i, o in enumerate(orcids.tolist())}      # bytes keys, exactly as stored
del orcids
print(f"{N:,} authors from {graph_dir}", flush=True)


def author_id(orcid):
    key = orcid.encode("utf-8")[:19]
    i = orcid_to_id.get(key)
    if i is None and key[-1:] in (b"X", b"x"):               # some records store the check digit as lowercase x
        i = orcid_to_id.get(key[:-1] + (b"x" if key[-1:] == b"X" else b"X"))
    return i


# ------------------------------------------------------------------ terms: topics and subfields from the vocabulary, keywords as they appear
terms = []                 # term id -> dict
topic_term = {}            # "T11464" -> term id
subfield_term = {}         # subfield name -> term id
keyword_term = {}          # lowercase keyword -> term id
topic_subfield = {}        # "T11464" -> subfield term id


def new_term(**rec):
    rec.setdefault("id", ""); rec.setdefault("subfield", ""); rec.setdefault("field", ""); rec.setdefault("domain", "")
    terms.append(rec)
    return len(terms) - 1


with gzip.open(vocab_path, "rt", encoding="utf-8") as f:
    for row in csv.DictReader(f, delimiter="\t"):
        tid = row["topic_id"]
        topic_term[tid] = new_term(name=row["name"], kind="topic", id=tid, subfield=row["subfield"], field=row["field"], domain=row["domain"])
        sf = row["subfield"]
        if sf and sf not in subfield_term:
            subfield_term[sf] = new_term(name=sf, kind="subfield", field=row["field"], domain=row["domain"])
        topic_subfield[tid] = subfield_term.get(sf)
print(f"{len(topic_term):,} topics, {len(subfield_term):,} subfields from {vocab_path}", flush=True)

# ------------------------------------------------------------------ pass 1: stream the works, spill (author, term) keys per partition
part_keys = [array.array("q") for _ in range(N_PARTITIONS)]
part_years = [array.array("h") for _ in range(N_PARTITIONS)]
part_files = [(open(os.path.join(tmp_dir, f"keys_{p}.bin"), "wb"), open(os.path.join(tmp_dir, f"years_{p}.bin"), "wb")) for p in range(N_PARTITIONS)]
term_pubs = array.array("q", [0]) * (1 << TERM_BITS)     # publications per term (grown lazily is not needed: fixed size)
seen_pmid = np.zeros(1, dtype=bool)
n_rows = n_pubs = n_pairs = n_unknown_authors = 0


def flush(p):
    part_files[p][0].write(part_keys[p].tobytes()); part_files[p][1].write(part_years[p].tobytes())
    del part_keys[p][:]; del part_years[p][:]


with gzip.open(topics_path, "rt", encoding="utf-8") as f:
    reader = csv.reader(f, delimiter="\t")
    next(reader)   # header
    for row in reader:
        n_rows += 1
        try:
            pmid = int(row[0])
        except ValueError:
            continue
        if pmid >= seen_pmid.shape[0]:
            grown = np.zeros(max(pmid + 1, 2 * seen_pmid.shape[0]), dtype=bool); grown[:seen_pmid.shape[0]] = seen_pmid; seen_pmid = grown
        if seen_pmid[pmid]:            # several OpenAlex works with the same PMID: first one counts
            continue
        seen_pmid[pmid] = True
        try:
            year = int(row[1]); year = year if 0 < year < 32767 else 0
        except (TypeError, ValueError):
            year = 0

        tids = set()
        for item in row[3].split(";"):
            tid = item.split(":")[0]
            t = topic_term.get(tid)
            if t is not None:
                tids.add(t)
                sf = topic_subfield.get(tid)
                if sf is not None:
                    tids.add(sf)
        for item in row[4].split(";"):
            name, _, score = item.rpartition(":")
            try:
                if not name or float(score) < KEYWORD_MIN_SCORE:
                    continue
            except ValueError:
                continue
            key = name.lower()
            t = keyword_term.get(key)
            if t is None:
                t = keyword_term[key] = new_term(name=name, kind="keyword")
                if len(terms) >= (1 << TERM_BITS):
                    raise SystemExit("more terms than TERM_BITS allows")
            tids.add(t)
        if not tids:
            continue
        aids = []
        for orcid in row[2].split(";"):
            if not orcid:
                continue
            a = author_id(orcid)
            if a is None:
                n_unknown_authors += 1
            else:
                aids.append(a)
        if not aids:
            continue
        n_pubs += 1
        for t in tids:
            term_pubs[t] += 1
        tids = list(tids)
        for a in set(aids):
            p = a * N_PARTITIONS // N
            base = a << TERM_BITS
            part_keys[p].extend([base | t for t in tids])
            part_years[p].extend([year] * len(tids))
            n_pairs += len(tids)
            if len(part_keys[p]) >= FLUSH:
                flush(p)
        if n_rows % 2_000_000 == 0:
            print(f"  {n_rows:,} rows, {n_pubs:,} publications, {n_pairs:,} author-term pairs, {len(terms):,} terms ({time.time() - t0:.0f}s)", flush=True)
for p in range(N_PARTITIONS):
    flush(p); part_files[p][0].close(); part_files[p][1].close()
T = len(terms)
print(f"pass 1: {n_rows:,} rows, {n_pubs:,} publications, {n_pairs:,} raw pairs, {T:,} terms "
      f"({len(keyword_term):,} keywords with score >= {KEYWORD_MIN_SCORE}); {n_unknown_authors:,} author entries not in the graph; {time.time() - t0:.0f}s", flush=True)
del orcid_to_id, seen_pmid

# ------------------------------------------------------------------ pass 2: aggregate every partition (author ranges are disjoint)
agg_keys, agg_counts, agg_years = [], [], []
for p in range(N_PARTITIONS):
    keys = np.fromfile(os.path.join(tmp_dir, f"keys_{p}.bin"), dtype=np.int64)
    years = np.fromfile(os.path.join(tmp_dir, f"years_{p}.bin"), dtype=np.int16)
    if keys.shape[0] == 0:
        continue
    order = np.argsort(keys, kind="stable")
    keys = keys[order]; years = years[order]; del order
    starts = np.flatnonzero(np.concatenate(([True], keys[1:] != keys[:-1])))
    counts = np.diff(np.append(starts, keys.shape[0]))
    agg_keys.append(keys[starts]); agg_counts.append(np.minimum(counts, 65535).astype(np.uint16)); agg_years.append(np.maximum.reduceat(years, starts))
    del keys, years, starts, counts
    os.remove(os.path.join(tmp_dir, f"keys_{p}.bin")); os.remove(os.path.join(tmp_dir, f"years_{p}.bin"))
    print(f"  partition {p + 1}/{N_PARTITIONS} aggregated: {agg_keys[-1].shape[0]:,} pairs ({time.time() - t0:.0f}s)", flush=True)
os.rmdir(tmp_dir)
keys = np.concatenate(agg_keys) if agg_keys else np.zeros(0, np.int64); counts = np.concatenate(agg_counts) if agg_counts else np.zeros(0, np.uint16)
years = np.concatenate(agg_years) if agg_years else np.zeros(0, np.int16)
del agg_keys, agg_counts, agg_years
P = int(keys.shape[0])

# ------------------------------------------------------------------ layout by term, strongest authors first
term_ids = (keys & ((1 << TERM_BITS) - 1)).astype(np.int32)
author_ids = (keys >> TERM_BITS).astype(np.int32)
del keys
order = np.lexsort((-counts.astype(np.int32), term_ids))
term_ids = term_ids[order]; author_ids = author_ids[order]; counts = counts[order]; years = years[order]
del order
term_off = np.zeros(T + 1, dtype=np.int64)
term_off[1:] = np.cumsum(np.bincount(term_ids, minlength=T), dtype=np.int64)
np.save(os.path.join(out_dir, "term_off.npy"), term_off)
np.save(os.path.join(out_dir, "term_authors.npy"), author_ids)
np.save(os.path.join(out_dir, "term_counts.npy"), counts)
np.save(os.path.join(out_dir, "term_years.npy"), years)
n_authors_per_term = np.diff(term_off)
for t, rec in enumerate(terms):
    rec["n_authors"] = int(n_authors_per_term[t]); rec["n_pubs"] = int(term_pubs[t])
json.dump(terms, open(os.path.join(out_dir, "terms.json"), "w"), ensure_ascii=False)
meta = {"terms": T, "topics": len(topic_term), "subfields": len(subfield_term), "keywords": len(keyword_term), "keyword_min_score": KEYWORD_MIN_SCORE,
        "publications": n_pubs, "author_term_pairs": P, "authors": N, "graph": os.path.abspath(graph_dir),
        "source": os.path.abspath(topics_path), "source_mtime": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(os.path.getmtime(topics_path))),
        "built": time.strftime("%Y-%m-%d %H:%M:%S"), "format": 1}
json.dump(meta, open(os.path.join(out_dir, "meta.json"), "w"), indent=1)
total = sum(os.path.getsize(os.path.join(out_dir, f)) for f in os.listdir(out_dir))
print(f"Author-topic table built in {out_dir}: {T:,} terms, {P:,} author-term pairs, {total / 2**30:.2f} GB, {time.time() - t0:.0f}s")
