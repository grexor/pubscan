"""Build the co-authorship graph and the title file from works.tab.gz (3_publications.py) and authors.tab.gz
(2_authors.py). The server (pubscan/graph.py) memory-maps these files; no database is involved.

usage: python db_graph.py [--openalex-names] [--pubmed-only] [--order-by-pmid]
                          [works.tab.gz [authors.tab.gz [output folder [orcid_records.tab.gz]]]]
       (defaults: works.tab.gz authors.tab.gz graph orcid_records.tab.gz)
  --openalex-names  name the authors as OpenAlex does, also when orcid_records.tab.gz is there
  --pubmed-only     keep only works with a PubMed id (the scope of pubScan up to v3.5)
  --order-by-pmid   number the publications by PubMed id instead of by date; with the two options above this
                    reproduces the earlier, PMID-keyed graph (used to compare the two)

Publications. A work counts as a publication when it has a PubMed id, or when its OpenAlex type is one of
PUBLICATION_TYPES, it is not paratext and it has a title. Of several works with the same PubMed id the first
one counts. Publications are numbered 0..M-1 from the oldest to the newest (publication date, then PubMed id,
then OpenAlex id; a year later than next year is not credible and is taken as unknown), so the newest
publications of an author are the ones with the highest numbers. These numbers are internal: they change with
every build. Towards the outside a publication is named by its OpenAlex work id.

The same paper twice. OpenAlex often holds a paper more than once (the journal article and a preprint or
repository copy). A work without a PubMed id is dropped when a better record with the same title carries all
of its authors and lies within SAME_PAPER_YEARS years. Better: has a PubMed id, else has a DOI and is not a
preprint, else has a DOI; among equals the record with more ORCID authors. Titles are compared by their letters
and digits only (case, accents, punctuation, markup tags and character entities do not count) and need
SAME_PAPER_TITLE of them. Nobody loses a paper this way: a second record that names an author whom the better
record lacks stays, and the paper then counts twice for the authors on both.

Exclusions. exclusions.tab (next to this script, or the file named by the environment variable EXCLUSIONS)
lists author-publication pairs of OpenAlex that are known to be wrong; those authors are left off those works.
The server reads the same file and leaves the pairs out at once; here they are left out of the build.

Authors. Every ORCID of authors.tab.gz that has a name and at least one publication, numbered 0..N-1 in sorted
ORCID order (the author id). Who wrote what is taken from OpenAlex as it is (exclusions.tab aside). The name is
the one the person states in the ORCID record (given names and family name; orcid_records.tab.gz, written by
orcid_records.py from the ORCID Public Data File): OpenAlex often has an abbreviated one ("A. Seljak"). Names
typed all in lower or all in upper case get their capitals put right, titles and degrees are removed ("Prof.
Dr.", "PhD"), and middle names or initials that OpenAlex knows are put in when the record has only a first and
a last name. The name of OpenAlex is used (of the author record with the most works, when several carry the
ORCID) when the ORCID record shows no name publicly, when its name is not in Latin letters, when it says less
than that of OpenAlex (a single word, or initials where OpenAlex has a first name), or when
orcid_records.tab.gz is not there. Names are stored transliterated to ASCII, as the name search expects.

Files (numpy .npy, little endian):
  orcids.npy           S19[N]       ORCID iDs (check digit X in upper case), sorted; author id = index
  names_blob.npy       uint8[B]     author names (UTF-8), concatenated in author id order
  names_off.npy        int64[N+1]   name of author i = names_blob[off[i]:off[i+1]]
  author_pubs.npy      int32[P]     publications per author, newest first
  author_pubs_off.npy  int64[N+1]   publications of author i = author_pubs[off[i]:off[i+1]]
  pub_authors.npy      int32[Q]     author ids per publication, in the paper's author order
  pub_authors_off.npy  int64[M+1]   authors of publication p = pub_authors[off[p]:off[p+1]]
  pub_year.npy         int16[M]     publication year, 0 = unknown
  pub_pmid.npy         int32[M]     PubMed id, 0 = none
  pub_work.npy         int64[M]     OpenAlex work id (the number after the W)
  titles_blob.npy      uint8[C]     the titles, compressed in blocks of TITLE_BLOCK publications (raw deflate with
                                    the dictionary below); a block holds its records joined by "\\n", a record is
                                    the title, followed by "\\t" and the DOI when the publication has no PubMed id
  titles_off.npy       int64[M/TITLE_BLOCK+1]   block b = titles_blob[off[b]:off[b+1]]
  titles_dict.npy      uint8[D]     deflate dictionary: frequent words of the titles
  meta.json            counts, scope, sources and build time
"""
import array
import collections
import csv
import gzip
import json
import multiprocessing as mp
import os
import re
import stat
import subprocess
import sys
import tempfile
import time
import unicodedata
import zlib

import numpy as np
from unidecode import unidecode

# OpenAlex work types that count as publications when a work has no PubMed id (works with one always count)
PUBLICATION_TYPES = {
    "article", "review", "preprint", "conference-paper", "conference-abstract", "book", "book-chapter",
    "book-review", "dissertation", "editorial", "letter", "report", "reference-entry",
}
SAME_PAPER_TITLE = 20     # two records can only be the same paper when their titles have this many letters and digits
SAME_PAPER_YEARS = 2      # ... and their years are at most this far apart
MAX_YEAR = time.localtime().tm_year + 1    # a later publication year is not credible and is taken as unknown
TITLE_BLOCK = 16          # publications per compressed block of the title file
DICT_SIZE = 32000         # bytes of the deflate dictionary (zlib uses at most 32 KB)
WORKERS = int(os.environ.get("WORKERS", "6"))
SORT_MEMORY = os.environ.get("SORT_MEMORY", "4G")

args = [a for a in sys.argv[1:] if not a.startswith("--")]
flags = {a for a in sys.argv[1:] if a.startswith("--")}
if flags - {"--openalex-names", "--pubmed-only", "--order-by-pmid"}:
    raise SystemExit(__doc__)
PUBMED_ONLY = "--pubmed-only" in flags
ORDER_BY_PMID = "--order-by-pmid" in flags
works_path = args[0] if len(args) > 0 else "works.tab.gz"
EXCLUSIONS = os.environ.get("EXCLUSIONS") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "exclusions.tab")
authors_path = args[1] if len(args) > 1 else "authors.tab.gz"
out_dir = args[2] if len(args) > 2 else "graph"
orcid_path = args[3] if len(args) > 3 else "orcid_records.tab.gz"
ORCID_NAMES = "--openalex-names" not in flags and os.path.exists(orcid_path)


def to_ascii(text):
    return unidecode(text).strip() if text else ""


LATIN = {}                # character -> is it a Latin letter (or no letter at all)?


def latin(text):
    """True when every letter of the text is a Latin one (accents allowed)."""
    for ch in text:
        if ch.isascii():
            continue
        known = LATIN.get(ch)
        if known is None:
            known = LATIN[ch] = not ch.isalpha() or unicodedata.name(ch, "").startswith("LATIN")
        if not known:
            return False
    return True


def proper_case(field, family=False):
    """A name field as typed into ORCID, with the capitals put right when it was typed all in lower case
    ("yunxia") or all in upper case ("SILVA OROZCO"). Mixed case stays as it is ("van Dijk", "McDonald"), and
    so do given names that consist of initials ("JM")."""
    letters = [ch for ch in field if ch.isalpha()]
    if len(letters) < 2:
        return field
    lower, upper = all(ch.islower() for ch in letters), all(ch.isupper() for ch in letters)
    if upper and not family and max(len(word) for word in re.split(r"[\s.\-]+", field)) < 3:
        return field                                          # initials
    if lower or upper:
        return re.sub(r"[^\W\d_]+", lambda word: word.group(0).capitalize(), field.lower())
    return field


# titles in front of a name and degrees behind it are no part of the name. Compared in lower case without dots.
TITLES = {"dr", "prof", "professor", "mr", "mrs", "miss", "sir", "pd", "priv", "doz", "privdoz", "dipl", "diplin", "assoc", "asst",
          "assist", "associate", "assistant", "univ", "apl", "em", "engr", "mudr", "rndr", "phdr", "judr", "mvdr", "paeddr", "dra", "drs"}
TITLES_WITH_DOT = {"doc", "ing", "mag", "mgr", "lic", "eng", "dott", "ir", "ms", "hab", "ass"}      # titles only when written with a dot
AFTER_A_TITLE = {"med", "rer", "nat", "phil", "habil", "hc", "ing", "sc", "techn", "iur", "oec", "h", "c"}   # "Dr. med.", "Dr. rer. nat.", "Dr. h. c."
DEGREES = {"phd", "dphil", "msc", "bsc", "mba", "dsc", "mph", "edd", "pharmd", "dds", "dvm", "mbbs", "frcs", "frcp", "mrcp", "facs"}
DEGREES_AFTER_COMMA = DEGREES | {"md", "do", "ms", "ma", "ba", "bs", "rn", "med", "mpd", "spd", "msi", "ssi", "sh", "mh", "st", "mt", "se",
                                 "mm", "skom", "mkom", "skep", "ns", "apt", "llm", "esq", "ing", "dr", "prof", "cpa", "pe"}


def without_titles(text):
    """A name without academic titles in front ("Prof. Dr. med. Anna Berg") and degrees behind ("Anna Berg, PhD",
    "Anna Berg MD"). "Md" in front stays: it is the usual short form of Mohammad. Generation suffixes stay
    ("Jr.", "III"); a comma that separated them goes."""
    words = text.replace(",", " , ").split()
    plain = lambda word: word.lower().replace(".", "").replace("-", "")
    first, seen_title = 0, False
    while first < len(words) - 1:
        word, key = words[first], plain(words[first])
        parts = [part for part in word.lower().replace(".", "").split("-") if part]       # "Dr.-Ing.", "Priv.-Doz.", "Univ.-Prof."
        joined = len(parts) > 1 and parts[0] in TITLES and all(part in TITLES or part in TITLES_WITH_DOT or part in AFTER_A_TITLE for part in parts)
        if key in TITLES or joined or (word.endswith(".") and key in TITLES_WITH_DOT) or (seen_title and key in AFTER_A_TITLE) or word in (",", "Ms"):
            seen_title = seen_title or word != ","
            first += 1
        else:
            break
    words = words[first:]
    if "," in words:                                          # behind a comma: degrees, in whatever notation
        cut = words.index(",")
        tail = [word for word in words[cut + 1:] if word != ","]
        if tail and all(plain(word) in DEGREES_AFTER_COMMA for word in tail):
            words = words[:cut]
    words = [word for word in words if word != ","]
    while len(words) > 2 and (plain(words[-1]) in DEGREES or words[-1] in ("MD", "M.D.", "MD.")):
        words.pop()
    return " ".join(words) if words else " ".join(text.replace(",", " ").split())


def record_name(given, family):
    """The name of an ORCID record as pubScan shows it: given names and family name as the person entered them,
    blanks tidied, capitals put right, titles and degrees removed. Empty when the record shows no name, or one
    that is not in Latin letters: transliterating those by rule gives wrong names (Japanese read as Chinese), so
    the name of OpenAlex, which is romanised, is kept for them."""
    given, family = " ".join(given.split()), " ".join(family.split())
    name = without_titles(" ".join(filter(None, (proper_case(given), proper_case(family, family=True)))))
    if not name or len(name) > 100 or not latin(name) or not any(ch.isalpha() for ch in name):
        return ""
    return name


def same_word(a, b):
    """Two name words that are the same apart from case, accents and dots."""
    fold = lambda word: unidecode(word).lower().replace(".", "")
    return fold(a) == fold(b)


def shown_name(given, family, openalex):
    """(name to show for an author, does it come from the ORCID record?). The name of the ORCID record
    (record_name) is taken, with two exceptions.
    The name of OpenAlex says more: the record holds a single word where OpenAlex has several ("Maria" against
    "Maria Rossi"), or it gives only initials where OpenAlex has a first name ("J. Smith" against "John Smith").
    Then the name of OpenAlex stays.
    The record holds a first and a last name and OpenAlex has the same two with middle names or initials between
    them ("Nicholas Luscombe" against "Nicholas M. Luscombe", "Mark Robinson" against "M. D. Robinson"): people
    often leave those out of their ORCID record, so they are put in.
    Titles and degrees are removed from either name."""
    theirs = without_titles(openalex).split()
    name = record_name(given, family)
    if not name:
        return " ".join(theirs), False
    mine = name.split()
    if len(mine) == 1 and len(theirs) > 1:
        return " ".join(theirs), False
    initials = [word for word in re.split(r"[\s.\-]+", " ".join(given.split())) if word]
    if family.strip() and initials and max(map(len, initials)) == 1 and theirs and len(theirs[0].rstrip(".")) > 2:
        return " ".join(theirs), False
    if len(mine) == 2 and 3 <= len(theirs) <= 5 and same_word(mine[1], theirs[-1]):
        first = theirs[0].rstrip(".")
        if same_word(mine[0], theirs[0]) or (len(first) == 1 and unidecode(mine[0])[:1].lower() == first.lower()):
            return " ".join([mine[0]] + theirs[1:-1] + [mine[1]]), True
    return name, True


def date_key(date, year):
    """YYYYMMDD as a number; a work with only a year sorts before the dated works of that year, unknown = 0."""
    if len(date) == 10 and date[4:5] == b"-" and date[7:8] == b"-":
        try:
            return int(date[0:4]) * 10000 + int(date[5:7]) * 100 + int(date[8:10])
        except ValueError:
            pass
    return year * 10000


NOT_LETTERS = bytes(b for b in range(128) if not (48 <= b <= 57 or 97 <= b <= 122))   # ASCII other than a-z and 0-9
MARKUP = re.compile(rb"</?[a-zA-Z][^<>]{0,200}>|&(?:[a-zA-Z]{2,8}|#[0-9]{1,6}|#[xX][0-9a-fA-F]{1,5});")   # tags, character entities


def title_letters(title):
    """The letters and digits of a title (UTF-8 bytes in, bytes out). Case, accents, punctuation of any script,
    markup tags and character entities do not count, so that the records of one paper meet whatever typography
    their sources use ("Earth's" and "Earth’s", "<i>in vivo</i>" and "in vivo", "régulation" and "regulation")."""
    if b"<" in title or b"&" in title:
        title = MARKUP.sub(b"", title)
    if title.isascii():
        return title.lower().translate(None, NOT_LETTERS)
    text = unicodedata.normalize("NFKD", title.decode("utf-8", "replace")).casefold()   # accents become separate marks
    return "".join(filter(str.isalnum, text)).encode("utf-8")                            # ... which are not letters


def title_key(title):
    """Hash of the letters and digits of a title, 0 when the title is too short to identify a paper."""
    letters = title_letters(title)
    return (hash(letters) or 1) if len(letters) >= SAME_PAPER_TITLE else 0


def load_exclusions(path):
    """Wrong author-publication pairs of OpenAlex (exclusions.tab): work id (digits) -> set of ORCID iDs (upper
    case) that are not authors of that work. One pair per line, tab or blank separated: work id with or without
    the W or the openalex.org address, ORCID iD with or without the orcid.org address; # starts a comment."""
    wrong = {}
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for line in f:
                cells = line.split("#")[0].split()
                if len(cells) < 2:
                    continue
                work = cells[0].rsplit("/", 1)[-1].lstrip("Ww")
                orcid = cells[1].rsplit("/", 1)[-1].upper()
                if work.isdigit() and len(orcid) == 19:
                    wrong.setdefault(work, set()).add(orcid)
    return wrong


def zcat(path):
    """The decompressed bytes of a gzip file as a pipe: (processes to wait for, stream). zcat runs in a process of
    its own; a regular file is read with direct I/O (dd), so that reading tens of GB does not push the data of a
    running server out of the page cache."""
    if stat.S_ISREG(os.stat(path).st_mode):
        dd = subprocess.Popen(["dd", "if=" + path, "iflag=direct", "bs=4M", "status=none"], stdout=subprocess.PIPE)
        unzip = subprocess.Popen(["zcat"], stdin=dd.stdout, stdout=subprocess.PIPE, bufsize=1 << 22)
        dd.stdout.close()
        return [unzip, dd], unzip.stdout
    unzip = subprocess.Popen(["zcat", path], stdout=subprocess.PIPE, bufsize=1 << 22)
    return [unzip], unzip.stdout


def save(name, data):
    np.save(os.path.join(out_dir, name + ".npy"), data)


def pack_blocks(job):
    """One contiguous range of title blocks: read its bytes from the raw title file, compress block by block.
    Returns (first block, part file, compressed block lengths)."""
    first, start, end, n_records, raw_path, dictionary, part_path = job
    with open(raw_path, "rb") as f:
        f.seek(start)
        records = f.read(end - start).split(b"\n")[:-1]     # every record ends with a line break
    assert len(records) == n_records
    lengths = []
    with open(part_path, "wb") as out:
        for i in range(0, n_records, TITLE_BLOCK):
            c = zlib.compressobj(9, zlib.DEFLATED, -15, 9, zlib.Z_DEFAULT_STRATEGY, dictionary)
            block = c.compress(b"\n".join(records[i:i + TITLE_BLOCK])) + c.flush()
            out.write(block)
            lengths.append(len(block))
    return first, part_path, lengths


if __name__ == "__main__":
    os.makedirs(out_dir, exist_ok=True)
    t0 = time.time()
    tmp = tempfile.mkdtemp(prefix="tmp_build_", dir=out_dir)
    stats = collections.Counter()

    # ------------------------------------------------------------ authors: ORCID -> name, sorted ORCID table
    print(f"Reading {authors_path} ...", flush=True)
    name_of = {}                                              # ORCID (bytes, check digit in upper case) -> name
    csv.field_size_limit(1 << 30)
    with gzip.open(authors_path, "rt", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter="\t")
        next(reader)                                          # header: openalex_id, name, orcid, alternatives, works_count
        works_of = {}                                         # ORCID -> works of the record that names it
        for row in reader:
            if len(row) < 3:
                continue
            orcid = row[2].replace("https://orcid.org/", "").strip().upper().encode("utf-8")[:19]
            name = row[1].strip()
            if orcid and name:
                works = int(row[4]) if len(row) > 4 and row[4].isdigit() else 0
                if works >= works_of.get(orcid, -1):          # several records with one ORCID: the one with the most
                    name_of[orcid] = name                     # works names it (the later one of equals)
                    works_of[orcid] = works
        del works_of
    n_records = len(name_of)

    # the ORCID records: the name as the person states it
    named_by_record = set()
    if ORCID_NAMES:
        print(f"Reading {orcid_path} ...", flush=True)
        with gzip.open(orcid_path, "rb") as f:
            next(f)                                           # header: orcid, given, family, credit, other, works
            for line in f:
                cells = line.split(b"\t", 3)
                if len(cells) < 4:
                    continue
                orcid = cells[0].upper()
                if orcid not in name_of:                      # no author of OpenAlex carries this ORCID
                    continue
                name, from_record = shown_name(cells[1].decode("utf-8", "replace"), cells[2].decode("utf-8", "replace"), name_of[orcid])
                if from_record and to_ascii(name):
                    name_of[orcid] = name
                    named_by_record.add(orcid)
        print(f"  {len(named_by_record):,} authors named by their ORCID record ({time.time() - t0:.0f}s)", flush=True)
    else:
        print(f"  the authors keep the names of OpenAlex ({'--openalex-names' if os.path.exists(orcid_path) else orcid_path + ' not found'})", flush=True)

    name_of = {o: a for o, a in ((o, to_ascii(without_titles(n))) for o, n in name_of.items()) if a}   # an author needs a name in ASCII
    all_orcids = sorted(name_of)
    candidate = {o: i for i, o in enumerate(all_orcids)}      # ORCID -> provisional id (sorted ORCID order)
    print(f"  {len(candidate):,} ORCIDs with a name ({n_records - len(candidate):,} without one in ASCII) in {time.time() - t0:.0f}s", flush=True)

    # ------------------------------------------------------------ pass 1: choose the publications, sort them
    # the chosen rows go through sort(1), keyed so that the output order is the publication numbering; their
    # authors stay in memory (row_authors) and join them again in pass 2
    print(f"Reading {works_path} ...", flush=True)
    env = dict(os.environ, LC_ALL="C")
    sorter = subprocess.Popen(["sort", "-t", "\t", "-k1,1", "-S", SORT_MEMORY, "--parallel=4", "-T", tmp],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=env, bufsize=1 << 22)
    readers, works_stream = zcat(works_path)
    exclusions = {work.encode(): {o.encode() for o in orcids} for work, orcids in load_exclusions(EXCLUSIONS).items()}
    print(f"  {sum(map(len, exclusions.values())):,} wrong author-publication pairs to leave out ({EXCLUSIONS})", flush=True)
    wrong_of = exclusions.get
    seen_pmid = np.zeros(1 << 26, dtype=bool)
    row_title = array.array("q")          # per chosen row: title hash, rank of the record, year, PubMed id
    row_rank = array.array("b")
    row_year = array.array("h")
    row_pmid = array.array("i")
    row_counts = array.array("i")         # ... the number of its author entries below
    row_authors = array.array("i")        # the (provisional) author ids of the chosen rows, row after row
    dropped_types = collections.Counter()
    type_codes = {}                       # work type -> small number (str for works without a PubMed id, bytes for those with one)
    row_type = array.array("b")           # per chosen row: the number of its type
    write = sorter.stdin.write
    lookup = candidate.get
    n_rows = n_chosen = 0                 # rows read, rows chosen (the hot counters are plain variables)
    first = True
    for line in works_stream:
        if first:                                             # header
            first = False
            continue
        r = line.rstrip(b"\n").split(b"\t")
        if len(r) != 11:
            stats["malformed rows"] += 1
            continue
        work_id, pmid, doi, wtype, wflags, year, date, n_authors, title, source, orcids = r
        n_rows += 1
        if n_rows % 10_000_000 == 0:
            print(f"  {n_rows:,} rows, {n_chosen:,} works chosen ({time.time() - t0:.0f}s)", flush=True)
        pmid = int(pmid) if pmid else 0
        if pmid == 0:
            if PUBMED_ONLY:
                continue
            wtype = wtype.decode("utf-8", "replace")
            if wtype not in PUBLICATION_TYPES or b"p" in wflags or not title:
                dropped_types[wtype if wtype not in PUBLICATION_TYPES else ("paratext" if b"p" in wflags else "no title")] += 1
                continue
        else:
            if pmid >= seen_pmid.shape[0]:
                grown = np.zeros(max(pmid + 1, 2 * seen_pmid.shape[0]), dtype=bool)
                grown[:seen_pmid.shape[0]] = seen_pmid
                seen_pmid = grown
            if seen_pmid[pmid]:                               # several OpenAlex works with the same PubMed id
                stats["works with a PubMed id already seen"] += 1
                continue
            seen_pmid[pmid] = True
            doi = b""                                         # the link of a PubMed publication goes to PubMed
        ids = []
        wrong = wrong_of(work_id)
        for o in orcids.split(b";"):
            o = o[:19].upper()
            if wrong and o in wrong:                          # exclusions.tab: not an author of this work
                stats["author entries left out by the exclusion file"] += 1
                continue
            i = lookup(o)
            if i is None:
                stats["author entries without a named ORCID record"] += 1
                continue
            ids.append(i)
        if not ids:
            stats["works without a known author"] += 1
            continue
        try:
            y = int(year) if year else 0
        except ValueError:
            y = 0
        if y > MAX_YEAR:
            stats["publication years later than next year, taken as unknown"] += 1
            y, date = 0, b""
        elif y < 0:
            y = 0
        work = int(work_id)
        key = b"%010d" % pmid if ORDER_BY_PMID else b"%08d%010d%011d" % (date_key(date, y), pmid, work)
        write(b"%s\t%d\t%d\t%d\t%d\t%s\t%s\n" % (key, n_chosen, work, pmid, y, doi, title))
        if not PUBMED_ONLY:
            row_title.append(title_key(title))
            row_rank.append(3 if pmid else (2 if wtype != "preprint" else 1) if doi else 0)
            row_year.append(y)
            row_pmid.append(pmid)
        row_counts.append(len(ids))
        row_authors.extend(ids)
        n_chosen += 1
        code = type_codes.get(wtype)
        if code is None:
            code = type_codes[wtype] = len(type_codes)
        row_type.append(code)
    if any(process.wait() != 0 for process in readers):
        raise SystemExit(f"reading {works_path} failed")
    sorter.stdin.close()
    del seen_pmid
    chosen_rows = n_chosen
    print(f"  {n_rows:,} rows, {chosen_rows:,} works chosen in {time.time() - t0:.0f}s", flush=True)

    counts = np.frombuffer(row_counts, dtype=np.int32)        # per chosen row: the number of its authors
    authors = np.frombuffer(row_authors, dtype=np.int32)      # authors of chosen row r = authors[row_start[r]:row_start[r + 1]]
    row_start = np.zeros(chosen_rows + 1, dtype=np.int64)
    row_start[1:] = np.cumsum(counts, dtype=np.int64)
    drop = np.zeros(chosen_rows, dtype=bool)

    # ------------------------------------------------------------ the same paper twice
    # records with the same title and a common author meet under one key (title hash combined with the author) and
    # the best record of every such group is found. A record without a PubMed id is dropped when one better record,
    # close in time, is the best one for every one of its authors: that record then carries all of its authors
    if not PUBMED_ONLY and chosen_rows:
        title = np.frombuffer(row_title, dtype=np.int64)
        rows = np.repeat(np.arange(chosen_rows, dtype=np.int32), counts)
        usable = title[rows] != 0
        rows = rows[usable]
        keys = title[rows].astype(np.uint64) ^ (authors[usable].astype(np.uint64) * np.uint64(0x9E3779B97F4A7C15))
        del usable, title
        # best record first: by rank, then the record that names more of its authors, then the earlier row
        score = np.frombuffer(row_rank, dtype=np.int8)[rows].astype(np.int64) * 4096 + np.minimum(counts[rows], 4095)
        score = score * chosen_rows + (chosen_rows - 1 - rows)
        order = np.lexsort((-score, keys))
        del score
        keys = keys[order]
        rows = rows[order]
        del order
        starts = np.concatenate(([True], keys[1:] != keys[:-1]))
        del keys
        winner = rows[np.maximum.accumulate(np.where(starts, np.arange(rows.shape[0]), 0))]
        del starts
        year = np.frombuffer(row_year, dtype=np.int16).astype(np.int32)
        same = (rows != winner) & (np.frombuffer(row_pmid, dtype=np.int32)[rows] == 0) & \
               ((year[rows] == 0) | (year[winner] == 0) | (np.abs(year[rows] - year[winner]) <= SAME_PAPER_YEARS))
        rows, winner = rows[same], winner[same]               # the author entries that a better record has as well
        del same, year
        covered = np.bincount(rows, minlength=chosen_rows)    # per record: how many of its author entries those are
        better = np.full(chosen_rows, -1, dtype=np.int32)
        better[rows] = winner                                 # one of the better records of the record ...
        only_one = np.ones(chosen_rows, dtype=bool)
        only_one[rows[better[rows] != winner]] = False        # ... and whether it is the same for all of its authors
        second = (covered > 0) & (covered == counts) & only_one
        stats["second records of a paper (same title, all authors on a better record)"] = int(second.sum())
        stats["records kept beside a better record of the same title that lacks one of their authors"] = int((covered > 0).sum()) - int(second.sum())
        del rows, winner, covered, better, only_one
    else:
        second = np.zeros(chosen_rows, dtype=bool)
    del row_title, row_rank, row_year, row_pmid

    def by_type(mask):
        """Work type -> number of chosen rows under the mask, most frequent first."""
        counted = np.bincount(np.frombuffer(row_type, dtype=np.int8)[mask], minlength=len(type_codes)) if chosen_rows else []
        total = collections.Counter()
        for wtype, code in type_codes.items():
            if counted[code]:
                total[wtype if isinstance(wtype, str) else wtype.decode("utf-8", "replace")] += int(counted[code])
        return dict(total.most_common())
    kept_types, second_types = by_type(~drop & ~second), by_type(second)
    del row_type
    drop |= second
    M = chosen_rows - int(drop.sum())
    print(f"  {int(second.sum()):,} second records of a paper dropped, {M:,} publications; sorting ... ({time.time() - t0:.0f}s)", flush=True)
    del second

    # ------------------------------------------------------------ pass 2: the publications in their final order
    pub_work = array.array("q")
    pub_pmid = array.array("i")
    pub_year = array.array("h")
    pub_counts = array.array("i")
    pub_authors = array.array("i")
    record_len = array.array("i")
    sample = []                                               # every 300th title, for the deflate dictionary
    raw_path = os.path.join(tmp, "titles.raw")
    j = -1
    with open(raw_path, "wb", buffering=1 << 24) as raw:
        for line in sorter.stdout:
            key, row, work, pmid, year, doi, title = line.rstrip(b"\n").split(b"\t")
            row = int(row)
            if drop[row]:
                continue
            j += 1
            pub_work.append(int(work))
            pub_pmid.append(int(pmid))
            pub_year.append(int(year))
            lo, hi = int(row_start[row]), int(row_start[row + 1])
            pub_counts.append(hi - lo)
            pub_authors.frombytes(authors[lo:hi].tobytes())
            record = title + b"\t" + doi if doi else title
            raw.write(record)
            raw.write(b"\n")
            record_len.append(len(record) + 1)
            if j % 300 == 0:
                sample.append(title)
            if (j + 1) % 10_000_000 == 0:
                print(f"  {j + 1:,} publications ordered ({time.time() - t0:.0f}s)", flush=True)
    if sorter.wait() != 0:
        raise SystemExit("sort failed")
    assert len(pub_work) == M
    del drop, row_start, authors, counts, row_authors, row_counts
    print(f"  {M:,} publications, {len(pub_authors):,} publication-author pairs in {time.time() - t0:.0f}s", flush=True)

    # ------------------------------------------------------------ authors with a publication get the final ids
    pub_authors = np.frombuffer(pub_authors, dtype=np.int32)
    counts = np.frombuffer(pub_counts, dtype=np.int32)
    has_pub = np.zeros(len(all_orcids), dtype=bool)
    has_pub[pub_authors] = True
    final_id = (np.cumsum(has_pub, dtype=np.int64) - 1).astype(np.int32)   # provisional id -> author id (valid where has_pub)
    N = int(has_pub.sum())
    kept = np.flatnonzero(has_pub)
    save("orcids", np.array([all_orcids[i] for i in kept.tolist()], dtype="S19"))
    name_bytes = [name_of[all_orcids[i]].encode("utf-8") for i in kept.tolist()]
    n_named_by_record = sum(1 for i in kept.tolist() if all_orcids[i] in named_by_record)
    del named_by_record
    names_off = np.zeros(N + 1, dtype=np.int64)
    names_off[1:] = np.cumsum([len(b) for b in name_bytes])
    save("names_blob", np.frombuffer(b"".join(name_bytes), dtype=np.uint8))
    save("names_off", names_off)
    del name_bytes, name_of, candidate, all_orcids

    pub_authors = final_id[pub_authors]
    del final_id, has_pub, kept
    pub_authors_off = np.zeros(M + 1, dtype=np.int64)
    pub_authors_off[1:] = np.cumsum(counts, dtype=np.int64)
    Q = int(pub_authors.shape[0])
    save("pub_authors", pub_authors)
    save("pub_authors_off", pub_authors_off)
    save("pub_year", np.frombuffer(pub_year, dtype=np.int16))
    save("pub_pmid", np.frombuffer(pub_pmid, dtype=np.int32))
    save("pub_work", np.frombuffer(pub_work, dtype=np.int64))
    n_pmid = int(np.count_nonzero(np.frombuffer(pub_pmid, dtype=np.int32)))
    del pub_authors_off, pub_year, pub_pmid, pub_work

    # publications per author, newest first: sort the pairs by (author, publication descending), drop repeats
    keys = np.repeat(np.arange(M - 1, -1, -1, dtype=np.int64), counts)       # M-1-p: ascending = newest first
    keys |= pub_authors.astype(np.int64) << 32
    del pub_authors, counts
    keys.sort()
    keys = keys[np.concatenate(([True], keys[1:] != keys[:-1]))]
    P = int(keys.shape[0])
    author_pubs_off = np.zeros(N + 1, dtype=np.int64)
    author_pubs_off[1:] = np.cumsum(np.bincount((keys >> 32).astype(np.int32), minlength=N), dtype=np.int64)
    save("author_pubs", (M - 1 - (keys & 0xFFFFFFFF)).astype(np.int32))
    save("author_pubs_off", author_pubs_off)
    del keys, author_pubs_off
    print(f"  {N:,} authors, {P:,} author-publication pairs written in {time.time() - t0:.0f}s", flush=True)

    # ------------------------------------------------------------ title file
    words = collections.Counter()
    for title in sample:
        for w in title.split(b" "):
            if len(w) >= 4:
                words[w] += 1
    chosen, size = [], 0
    for w, n in sorted(words.items(), key=lambda x: (-x[1] * (len(x[0]) - 2), x[0])):   # bytes saved by the word
        if n < 2:
            continue
        if size + len(w) + 1 > DICT_SIZE:
            break
        chosen.append(w)
        size += len(w) + 1
    dictionary = b" ".join(reversed(chosen)) or b" "         # the most valuable words closest to the data
    save("titles_dict", np.frombuffer(dictionary, dtype=np.uint8))
    del sample, words

    record_len = np.frombuffer(record_len, dtype=np.int32)
    n_blocks = (M + TITLE_BLOCK - 1) // TITLE_BLOCK
    record_off = np.zeros(M + 1, dtype=np.int64)
    record_off[1:] = np.cumsum(record_len, dtype=np.int64)
    per_job = 50_000                                          # blocks per job (800,000 titles, about 70 MB of text)
    jobs = []
    for b in range(0, n_blocks, per_job):
        p0, p1 = b * TITLE_BLOCK, min((b + per_job) * TITLE_BLOCK, M)
        jobs.append((b, int(record_off[p0]), int(record_off[p1]), p1 - p0, raw_path, dictionary, os.path.join(tmp, f"titles_{b:09d}.part")))
    del record_off, record_len
    block_len = np.zeros(n_blocks, dtype=np.int64)
    parts = {}
    with mp.get_context("fork").Pool(WORKERS) as pool:
        for b, part_path, lengths in pool.imap_unordered(pack_blocks, jobs):
            block_len[b:b + len(lengths)] = lengths
            parts[b] = part_path
    titles_off = np.zeros(n_blocks + 1, dtype=np.int64)
    titles_off[1:] = np.cumsum(block_len)
    C = int(titles_off[-1])
    blob = np.lib.format.open_memmap(os.path.join(out_dir, "titles_blob.npy"), mode="w+", dtype=np.uint8, shape=(C,))
    for b in sorted(parts):
        data = np.fromfile(parts[b], dtype=np.uint8)
        blob[int(titles_off[b]):int(titles_off[b]) + data.shape[0]] = data
        os.remove(parts[b])
    blob.flush()
    del blob
    save("titles_off", titles_off)
    raw_bytes = os.path.getsize(raw_path)
    os.remove(raw_path)
    os.rmdir(tmp)
    print(f"  titles: {raw_bytes / 2**30:.2f} GB of text in {C / 2**30:.2f} GB ({C / max(M, 1):.1f} bytes per publication) in {time.time() - t0:.0f}s", flush=True)

    meta = {"format": 2, "authors": N, "publications": M, "publications_with_pmid": n_pmid, "author_pub_pairs": P, "pub_author_pairs": Q,
            "title_block": TITLE_BLOCK, "scope": "pubmed" if PUBMED_ONLY else "all", "order": "pmid" if ORDER_BY_PMID else "date",
            "names": {"as stated in the ORCID record": n_named_by_record, "from OpenAlex": N - n_named_by_record},
            "orcid_records": os.path.abspath(orcid_path) if ORCID_NAMES else None,
            "publication_types": sorted(PUBLICATION_TYPES), "publications_by_type": kept_types, "second_records_by_type": second_types,
            "dropped": dict(dropped_types.most_common()), "notes": dict(stats),
            "works_rows": n_rows,
            "source": os.path.abspath(works_path), "source_mtime": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(os.path.getmtime(works_path))),
            "authors_source": os.path.abspath(authors_path), "exclusions": sum(map(len, exclusions.values())), "built": time.strftime("%Y-%m-%d %H:%M:%S")}
    json.dump(meta, open(os.path.join(out_dir, "meta.json"), "w"), indent=1)
    total = sum(os.path.getsize(os.path.join(out_dir, f)) for f in os.listdir(out_dir))
    print(f"Graph built in {out_dir}: {N:,} authors, {M:,} publications ({n_pmid:,} with a PubMed id), {total / 2**30:.2f} GB, {time.time() - t0:.0f}s")
