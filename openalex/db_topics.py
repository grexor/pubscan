"""Build the author-topic table from works_topics.tab.gz (written by 3_publications.py).

usage: python db_topics.py <works_topics.tab.gz> <works_topics_vocab.tab.gz> <graph dir> <output folder>
       (defaults: works_topics.tab.gz works_topics_vocab.tab.gz graph topics)

OpenAlex gives every work up to three topics from a fixed taxonomy (each with a subfield, field and domain)
and about ten scored keywords. This script counts, for every author and every term, the number of the
author's publications tagged with that term, and stores the result term by term so that "who publishes
on X" is one slice of an array, and once more author by author for "what does this author publish on".
Terms are the topics, their subfields and the keywords with a score of at least KEYWORD_MIN_SCORE.
Only works that are publications of the graph are counted, and the authors of a publication are taken from
the graph as well (author id = position in graph/orcids.npy): db_graph.py alone decides what is a publication
and who is an author of it (exclusions.tab), so this table cannot disagree with the graph.

Files (numpy .npy, little endian) in the output folder:
  term_off.npy      int64[T+1]   authors of term t = slice off[t]:off[t+1] of the three arrays below
  term_authors.npy  int32[P]     graph author ids, ordered by publication count (descending) within a term
  term_counts.npy   uint16[P]    the author's number of publications carrying the term
  term_years.npy    int16[P]     year of the author's newest publication carrying the term (0 = unknown)
  author_off.npy    int64[N+1]   terms of author a = slice off[a]:off[a+1] of the three arrays below (same pairs, by author)
  author_terms.npy  int32[P]     term ids, ordered by publication count (descending, newest first among equals) within an author
  author_counts.npy uint16[P]
  author_years.npy  int16[P]
  terms.json        list of T terms: {"name", "kind": "topic"|"subfield"|"keyword", "id", "subfield", "field",
                                      "domain", "n_authors", "n_pubs"}; the list index is the term id
  meta.json         counts, source files and build time

Memory: the (author, term) pairs never sit in memory all at once. They are spilled to disk by author range,
aggregated range by range, written out by author, spilled again by term range and written out by term.
"""
import sys
import os
import io
import gzip
import json
import stat
import time
import array
import subprocess
import numpy as np

topics_path = sys.argv[1] if len(sys.argv) > 1 else "works_topics.tab.gz"
vocab_path = sys.argv[2] if len(sys.argv) > 2 else "works_topics_vocab.tab.gz"
graph_dir = sys.argv[3] if len(sys.argv) > 3 else "graph"
out_dir = sys.argv[4] if len(sys.argv) > 4 else "topics"
KEYWORD_MIN_SCORE = float(os.environ.get("KEYWORD_MIN_SCORE", "0.5"))
N_PARTITIONS = 32                 # author ranges (pass 1 and 2) and term ranges (pass 3)
TERM_BITS = 24                    # pair key = author id << TERM_BITS | term id
FLUSH = 20_000_000                # pairs collected before they are written to the files of their author ranges

os.makedirs(out_dir, exist_ok=True)
tmp_dir = os.path.join(out_dir, "tmp_pairs")
os.makedirs(tmp_dir, exist_ok=True)
t0 = time.time()

# ------------------------------------------------------------------ graph: the publications and their authors
N = int(np.load(os.path.join(graph_dir, "orcids.npy"), mmap_mode="r").shape[0])
pub_work = np.load(os.path.join(graph_dir, "pub_work.npy"))
by_work = np.argsort(pub_work)                    # publication numbers in the order of their OpenAlex work ids
publications = pub_work[by_work]                  # the work ids, sorted: a work is looked up here ...
del pub_work
pub_authors = np.load(os.path.join(graph_dir, "pub_authors.npy"))            # ... and its authors are taken from the graph:
pub_authors_off = np.load(os.path.join(graph_dir, "pub_authors_off.npy"))    # the authors that db_graph.py counts, no others
M = int(publications.shape[0])
print(f"{N:,} authors, {M:,} publications from {graph_dir}", flush=True)


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


with gzip.open(vocab_path, "rt", encoding="utf-8", newline="\n") as f:
    next(f)                # header: topic_id, name, subfield, field, domain
    for line in f:
        tid, name, sf, field, domain = (line.rstrip("\n").split("\t") + ["", "", "", ""])[:5]
        topic_term[tid] = new_term(name=name, kind="topic", id=tid, subfield=sf, field=field, domain=domain)
        if sf and sf not in subfield_term:
            subfield_term[sf] = new_term(name=sf, kind="subfield", field=field, domain=domain)
        topic_subfield[tid] = subfield_term.get(sf)
print(f"{len(topic_term):,} topics, {len(subfield_term):,} subfields from {vocab_path}", flush=True)

# ------------------------------------------------------------------ pass 1: stream the works, spill (author, term) keys by author range
# The rows are read in batches: which of them are publications of the graph is one array lookup per batch, and the
# kept rows are only collected (their authors, their terms); the pairs are formed with array operations in flush().
part_files = [(open(os.path.join(tmp_dir, f"keys_{p}.bin"), "wb"), open(os.path.join(tmp_dir, f"years_{p}.bin"), "wb")) for p in range(N_PARTITIONS)]
term_pubs = np.zeros(1 << TERM_BITS, dtype=np.int64)     # publications per term
buf_authors, buf_authors_n = array.array("i"), array.array("i")     # collected rows: their authors, authors per row
buf_terms, buf_terms_n = array.array("i"), array.array("i")         # their terms, terms per row
buf_years = array.array("h")
buffered = 0                                                        # pairs the collected rows expand to
n_rows = n_pubs = n_pairs = n_other_works = 0
NO_WORK = -(1 << 62)


def work_number(row):
    if len(row) != 5:
        return NO_WORK
    try:
        return int(row[0])
    except ValueError:
        return NO_WORK


def flush():
    """Pair every author of a collected row with every term of that row and append the pairs (key = author and
    term, and the year of the publication) to the files of their author ranges."""
    global buffered
    if not len(buf_years):
        return
    authors_n = np.frombuffer(buf_authors_n, dtype=np.int32).astype(np.int64)
    terms_n = np.frombuffer(buf_terms_n, dtype=np.int32).astype(np.int64)
    row_authors = np.frombuffer(buf_authors, dtype=np.int32).astype(np.int64)
    row_terms = np.frombuffer(buf_terms, dtype=np.int32).copy()
    row_years = np.frombuffer(buf_years, dtype=np.int16).copy()
    for buf in (buf_authors, buf_authors_n, buf_terms, buf_terms_n, buf_years):   # copied: the buffers can be emptied
        del buf[:]
    counts = np.bincount(row_terms)
    term_pubs[:counts.shape[0]] += counts
    reps = np.repeat(terms_n, authors_n)                               # per author entry: the number of terms of its row
    authors = np.repeat(row_authors, reps)                             # per pair
    first_term = np.repeat(np.cumsum(terms_n) - terms_n, authors_n)    # per author entry: where the terms of its row start
    first_pair = np.cumsum(reps) - reps                                # per author entry: its first pair
    terms = row_terms[np.repeat(first_term - first_pair, reps) + np.arange(authors.shape[0], dtype=np.int64)]
    years = np.repeat(np.repeat(row_years, authors_n), reps)
    assert authors.shape[0] == buffered
    keys = (authors << TERM_BITS) | terms
    ranges = authors * N_PARTITIONS // N
    del authors, terms, reps, first_term, first_pair
    order = np.argsort(ranges, kind="stable")
    bounds = np.searchsorted(ranges[order], np.arange(N_PARTITIONS + 1))
    keys, years = keys[order], years[order]
    for p in range(N_PARTITIONS):
        lo, hi = int(bounds[p]), int(bounds[p + 1])
        if hi > lo:
            part_files[p][0].write(keys[lo:hi].tobytes()); part_files[p][1].write(years[lo:hi].tobytes())
    buffered = 0


# zcat runs in a process of its own; a regular file is read with direct I/O (dd), so that reading tens of GB does
# not push the data of a running server out of the page cache
if stat.S_ISREG(os.stat(topics_path).st_mode):
    dd = subprocess.Popen(["dd", "if=" + topics_path, "iflag=direct", "bs=4M", "status=none"], stdout=subprocess.PIPE)
    unzip = subprocess.Popen(["zcat"], stdin=dd.stdout, stdout=subprocess.PIPE, bufsize=1 << 22)
    dd.stdout.close()
    readers = [unzip, dd]
else:
    unzip = subprocess.Popen(["zcat", topics_path], stdout=subprocess.PIPE, bufsize=1 << 22)
    readers = [unzip]
with io.TextIOWrapper(unzip.stdout, encoding="utf-8", newline="\n") as f:
    next(f)                # header: work_id, year, orcids, topics, keywords
    while True:
        lines = f.readlines(1 << 25)              # about 32 MB of text
        if not lines:
            break
        rows = [line.rstrip("\n").split("\t") for line in lines]
        works = np.fromiter(map(work_number, rows), dtype=np.int64, count=len(rows))
        at = np.minimum(np.searchsorted(publications, works), max(M - 1, 0))
        member = publications[at] == works if M else np.zeros(len(rows), dtype=bool)   # a publication of the graph? (not: type, second record of a paper, ...)
        n_other_works += int((works != NO_WORK).sum()) - int(member.sum())
        kept = np.flatnonzero(member)
        for row, p in zip([rows[i] for i in kept.tolist()], by_work[at[kept]].tolist()):     # p: the publication number
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
            aids = set(pub_authors[pub_authors_off[p]:pub_authors_off[p + 1]].tolist())   # an author listed twice counts once
            n_pubs += 1
            buf_authors.extend(aids); buf_authors_n.append(len(aids))
            buf_terms.extend(tids); buf_terms_n.append(len(tids))
            buf_years.append(year)
            buffered += len(aids) * len(tids)
        before = n_rows
        n_rows += len(rows)
        if buffered >= FLUSH:
            n_pairs += buffered
            flush()
        if n_rows // 5_000_000 != before // 5_000_000:
            print(f"  {n_rows:,} rows, {n_pubs:,} publications, {n_pairs + buffered:,} author-term pairs, {len(terms):,} terms ({time.time() - t0:.0f}s)", flush=True)
if any(process.wait() != 0 for process in readers):
    raise SystemExit(f"reading {topics_path} failed")
n_pairs += buffered
flush()
for p in range(N_PARTITIONS):
    part_files[p][0].close(); part_files[p][1].close()
T = len(terms)
print(f"pass 1: {n_rows:,} rows, {n_pubs:,} publications, {n_pairs:,} raw pairs, {T:,} terms "
      f"({len(keyword_term):,} keywords with score >= {KEYWORD_MIN_SCORE}); {n_other_works:,} works that are not publications of the graph; "
      f"{time.time() - t0:.0f}s", flush=True)
del publications, by_work, pub_authors, pub_authors_off

# ------------------------------------------------------------------ pass 2: aggregate every author range; count the pairs per author and per term
TERM_MASK = (1 << TERM_BITS) - 1
terms_of_author = np.zeros(N, dtype=np.int64)
authors_of_term = np.zeros(T, dtype=np.int64)
part_pairs = []
for p in range(N_PARTITIONS):
    keys = np.fromfile(os.path.join(tmp_dir, f"keys_{p}.bin"), dtype=np.int64)
    years = np.fromfile(os.path.join(tmp_dir, f"years_{p}.bin"), dtype=np.int16)
    os.remove(os.path.join(tmp_dir, f"keys_{p}.bin")); os.remove(os.path.join(tmp_dir, f"years_{p}.bin"))
    part_pairs.append(0)
    if keys.shape[0] == 0:
        continue
    order = np.argsort(keys, kind="stable")
    keys = keys[order]; years = years[order]; del order
    starts = np.flatnonzero(np.concatenate(([True], keys[1:] != keys[:-1])))
    counts = np.minimum(np.diff(np.append(starts, keys.shape[0])), 65535).astype(np.uint16)
    years = np.maximum.reduceat(years, starts)
    keys = keys[starts]; del starts
    terms_of_author += np.bincount(keys >> TERM_BITS, minlength=N)
    authors_of_term += np.bincount(keys & TERM_MASK, minlength=T)
    np.save(os.path.join(tmp_dir, f"agg_keys_{p}.npy"), keys); np.save(os.path.join(tmp_dir, f"agg_counts_{p}.npy"), counts); np.save(os.path.join(tmp_dir, f"agg_years_{p}.npy"), years)
    part_pairs[p] = int(keys.shape[0])
    del keys, counts, years
    print(f"  author range {p + 1}/{N_PARTITIONS} aggregated: {part_pairs[p]:,} pairs ({time.time() - t0:.0f}s)", flush=True)
P = sum(part_pairs)


def new_array(name, dtype):
    return np.lib.format.open_memmap(os.path.join(out_dir, name + ".npy"), mode="w+", dtype=dtype, shape=(P,))


# ------------------------------------------------------------------ pass 3: layout by author (strongest term first), spill by term range
author_off = np.zeros(N + 1, dtype=np.int64)
author_off[1:] = np.cumsum(terms_of_author)
np.save(os.path.join(out_dir, "author_off.npy"), author_off)
del author_off, terms_of_author
out_terms, out_counts, out_years = new_array("author_terms", np.int32), new_array("author_counts", np.uint16), new_array("author_years", np.int16)
spill = [[open(os.path.join(tmp_dir, f"term_{kind}_{q}.bin"), "wb") for kind in ("terms", "authors", "counts", "years")] for q in range(N_PARTITIONS)]
pos = 0
for p in range(N_PARTITIONS):
    if part_pairs[p] == 0:
        continue
    keys = np.load(os.path.join(tmp_dir, f"agg_keys_{p}.npy")); counts = np.load(os.path.join(tmp_dir, f"agg_counts_{p}.npy")); years = np.load(os.path.join(tmp_dir, f"agg_years_{p}.npy"))
    for kind in ("keys", "counts", "years"):
        os.remove(os.path.join(tmp_dir, f"agg_{kind}_{p}.npy"))
    term_ids = (keys & TERM_MASK).astype(np.int32)
    author_ids = (keys >> TERM_BITS).astype(np.int32)
    del keys
    # for the layout by term: the pairs in author order, split by term range (stable, so a term's authors stay in author order)
    ranges = term_ids.astype(np.int64) * N_PARTITIONS // T
    for q in np.unique(ranges).tolist():
        pick = ranges == q
        spill[q][0].write(term_ids[pick].tobytes()); spill[q][1].write(author_ids[pick].tobytes())
        spill[q][2].write(counts[pick].tobytes()); spill[q][3].write(years[pick].tobytes())
    del ranges
    order = np.lexsort((-years.astype(np.int32), -counts.astype(np.int32), author_ids))
    n = order.shape[0]
    out_terms[pos:pos + n] = term_ids[order]; out_counts[pos:pos + n] = counts[order]; out_years[pos:pos + n] = years[order]
    pos += n
    del order, term_ids, author_ids, counts, years
assert pos == P
for a in (out_terms, out_counts, out_years):
    a.flush()
del out_terms, out_counts, out_years
for files in spill:
    for f in files:
        f.close()
print(f"  layout by author written: {P:,} pairs ({time.time() - t0:.0f}s)", flush=True)

# ------------------------------------------------------------------ pass 4: layout by term, strongest authors first
term_off = np.zeros(T + 1, dtype=np.int64)
term_off[1:] = np.cumsum(authors_of_term)
np.save(os.path.join(out_dir, "term_off.npy"), term_off)
out_authors, out_counts, out_years = new_array("term_authors", np.int32), new_array("term_counts", np.uint16), new_array("term_years", np.int16)
pos = 0
for q in range(N_PARTITIONS):
    names = [os.path.join(tmp_dir, f"term_{kind}_{q}.bin") for kind in ("terms", "authors", "counts", "years")]
    term_ids = np.fromfile(names[0], dtype=np.int32); author_ids = np.fromfile(names[1], dtype=np.int32)
    counts = np.fromfile(names[2], dtype=np.uint16); years = np.fromfile(names[3], dtype=np.int16)
    for name in names:
        os.remove(name)
    order = np.lexsort((-counts.astype(np.int32), term_ids))
    n = order.shape[0]
    if n:
        assert pos == term_off[int(term_ids[order[0]])]
    out_authors[pos:pos + n] = author_ids[order]; out_counts[pos:pos + n] = counts[order]; out_years[pos:pos + n] = years[order]
    pos += n
    del order, term_ids, author_ids, counts, years
assert pos == P
for a in (out_authors, out_counts, out_years):
    a.flush()
del out_authors, out_counts, out_years
os.rmdir(tmp_dir)

for t, rec in enumerate(terms):
    rec["n_authors"] = int(authors_of_term[t]); rec["n_pubs"] = int(term_pubs[t])
json.dump(terms, open(os.path.join(out_dir, "terms.json"), "w"), ensure_ascii=False)
meta = {"terms": T, "topics": len(topic_term), "subfields": len(subfield_term), "keywords": len(keyword_term), "keyword_min_score": KEYWORD_MIN_SCORE,
        "publications": n_pubs, "author_term_pairs": P, "authors": N, "graph": os.path.abspath(graph_dir),
        "source": os.path.abspath(topics_path), "source_mtime": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(os.path.getmtime(topics_path))),
        "built": time.strftime("%Y-%m-%d %H:%M:%S"), "format": 2, "author_layout": True}
json.dump(meta, open(os.path.join(out_dir, "meta.json"), "w"), indent=1)
total = sum(os.path.getsize(os.path.join(out_dir, f)) for f in os.listdir(out_dir))
print(f"Author-topic table built in {out_dir}: {T:,} terms, {P:,} author-term pairs, {total / 2**30:.2f} GB, {time.time() - t0:.0f}s")
