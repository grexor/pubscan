"""Memory-mapped co-authorship graph and title file built by openalex/db_graph.py.

All arrays are opened read-only with numpy's mmap, so the operating system shares one copy
between processes, pages load on first use and nothing has to be parsed per request. Every
method returns views or small Python objects; the object is safe to share between threads.

Publications are numbered 0..M-1 from the oldest to the newest. The numbers are internal to
one build of the files; towards the outside a publication is named by its OpenAlex work id.

Exclusions. OpenAlex attaches publications to authors partly by matching names, so a list can hold
a paper of a namesake. openalex/exclusions.tab names such pairs (OpenAlex work id, ORCID iD); the
graph answers as if the author were not on the publication. The file is read again when it has
changed, so a new line takes effect within seconds, without building anything.

Hidden authors. OpenAlex sometimes keeps an author record that mixes the papers of several people under an
ORCID iD whose owner claims none of them. openalex/hidden_authors.tab names such ORCID iDs; the graph answers as
if the author had no publications (no network, not a co-author of anybody), and the server leaves the author out
of the name suggestions and the topic lists (Graph.hidden, Graph.hidden_orcids). Read again like the exclusions.

Affiliations. openalex/db_affiliations.py adds the main affiliation of every author to the folder (the institution
on the author's most recent works in OpenAlex). The files are optional: without them, or when they were built for
another set of authors, Graph.affiliation() answers None.
"""
import os
import json
import threading
import time
import zlib
import numpy as np

# next to the pipeline that also reads it: <repository>/openalex/exclusions.tab
EXCLUSIONS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "openalex", "exclusions.tab")
HIDDEN = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "openalex", "hidden_authors.tab")


def read_hidden(path):
    """ORCID iDs (upper case) of the hidden authors file: one per line, with or without the orcid.org address,
    anything after it is a note; # starts a comment."""
    orcids = []
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                cells = line.split("#")[0].split()
                if cells and len(cells[0].rsplit("/", 1)[-1]) == 19:
                    orcids.append(cells[0].rsplit("/", 1)[-1].upper())
    except OSError:
        pass
    return orcids


def read_exclusions(path):
    """[(work id, ORCID iD), ...] of the exclusion file: tab or blank separated, work id with or without the W
    or the openalex.org address, ORCID iD with or without the orcid.org address; # starts a comment."""
    pairs = []
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                cells = line.split("#")[0].split()
                if len(cells) < 2:
                    continue
                work = cells[0].rsplit("/", 1)[-1].lstrip("Ww")
                orcid = cells[1].rsplit("/", 1)[-1].upper()
                if work.isdigit() and len(orcid) == 19:
                    pairs.append((int(work), orcid))
    except OSError:
        pass
    return pairs


class Graph:

    FORMAT = 2
    FILES = ("orcids", "names_blob", "names_off", "author_pubs", "author_pubs_off", "pub_authors", "pub_authors_off",
             "pub_year", "pub_pmid", "pub_work", "titles_blob", "titles_off", "titles_dict")

    AFFILIATION_FILES = ("institutions_blob", "institutions_off", "author_institution", "author_institution_year")   # optional

    def __init__(self, folder, exclusions=EXCLUSIONS, hidden=HIDDEN):
        self.folder = folder
        self.exclusions_path = exclusions
        self.hidden_path = hidden
        self.hidden = frozenset()       # author ids of the hidden authors
        self.hidden_orcids = frozenset()    # their ORCID iDs, upper case
        self.excluded_pubs = {}         # author id -> publications that do not count for the author (int32 array)
        self.excluded_authors = {}      # publication -> authors that do not count on it (int32 array)
        self.exclusions = []            # the pairs in force: (work id, ORCID iD)
        self._exclusions_file = None    # (modification time, size) of the file when it was read
        self._exclusions_looked = 0.0
        self._exclusions_lock = threading.Lock()
        self.meta = json.load(open(os.path.join(folder, "meta.json")))
        if self.meta.get("format") != self.FORMAT:
            raise ValueError(f"{folder}: graph files of format {self.meta.get('format')}, this server reads format {self.FORMAT} (rebuild with openalex/db_graph.py)")
        for name in self.FILES:
            setattr(self, name, np.load(os.path.join(folder, name + ".npy"), mmap_mode="r"))
        self.n_authors = int(self.orcids.shape[0])
        self.n_pubs = int(self.pub_year.shape[0])
        self.title_block = int(self.meta["title_block"])
        self.dictionary = self.titles_dict.tobytes()
        self.has_affiliations = all(os.path.isfile(os.path.join(folder, name + ".npy")) for name in self.AFFILIATION_FILES)
        if self.has_affiliations:
            for name in self.AFFILIATION_FILES:
                setattr(self, name, np.load(os.path.join(folder, name + ".npy"), mmap_mode="r"))
            self.has_affiliations = int(self.author_institution.shape[0]) == self.n_authors    # built for these authors
        self.refresh_exclusions(every=0)

    # ---- exclusions
    def refresh_exclusions(self, every=10.0):
        """Read the exclusion file and the hidden authors file again if one has changed. Called with every request;
        looks at the files at most once in `every` seconds, and only one thread at a time does the reading."""
        now = time.monotonic()
        if now - self._exclusions_looked < every or not self._exclusions_lock.acquire(blocking=False):
            return
        try:
            self._exclusions_looked = now
            seen = []
            for path in (self.exclusions_path, self.hidden_path):
                try:
                    st = os.stat(path)
                    seen.append((st.st_mtime_ns, st.st_size))
                except (OSError, TypeError):
                    seen.append(None)
            seen = tuple(seen)
            if seen == self._exclusions_file:
                return
            pairs = read_exclusions(self.exclusions_path) if seen[0] else []
            pubs, authors, found = {}, {}, []
            hidden = {}
            for orcid in (read_hidden(self.hidden_path) if seen[1] else []):
                a = self.lookup(orcid)
                if a is not None:                    # a hidden author: every publication is one the author is not on
                    hidden[a] = self.orcid(a).upper()
                    for p in self.author_pubs[self.author_pubs_off[a]:self.author_pubs_off[a + 1]].tolist():
                        pubs.setdefault(a, set()).add(p)
                        authors.setdefault(p, set()).add(a)
            if pairs:
                works = np.array(sorted({w for w, _ in pairs}), dtype=np.int64)
                where = np.flatnonzero(np.isin(self.pub_work, works))          # one pass over the publications
                pub_of = {int(self.pub_work[p]): int(p) for p in where.tolist()}
                for work, orcid in pairs:
                    a, p = self.lookup(orcid), pub_of.get(work)
                    if a is not None and p is not None:
                        pubs.setdefault(a, set()).add(p)
                        authors.setdefault(p, set()).add(a)
                        found.append((work, orcid))
            # whole dictionaries are put in place at once: a request sees the old or the new ones
            self.excluded_pubs = {a: np.array(sorted(s), dtype=np.int32) for a, s in pubs.items()}
            self.excluded_authors = {p: np.array(sorted(s), dtype=np.int32) for p, s in authors.items()}
            self.exclusions = found
            self.hidden = frozenset(hidden)
            self.hidden_orcids = frozenset(hidden.values())
            self._exclusions_file = seen
        finally:
            self._exclusions_lock.release()

    # ---- authors
    def id(self, orcid):
        """Author id for an ORCID spelled exactly as stored, or None."""
        key = np.bytes_(orcid.encode("utf-8")[:19])
        i = int(np.searchsorted(self.orcids, key))
        if i < self.n_authors and self.orcids[i] == key:
            return i
        return None

    def lookup(self, orcid):
        """Author id for an ORCID in any check-digit spelling: the given one first, then the
        other case of a trailing X/x (a share of the records store a lowercase x)."""
        i = self.id(orcid)
        if i is None and orcid[-1:] in ("X", "x"):
            i = self.id(orcid[:-1] + ("x" if orcid[-1] == "X" else "X"))
        return i

    def orcid(self, i):
        return self.orcids[i].decode("utf-8")

    def name(self, i):
        return self.names_blob[self.names_off[i]:self.names_off[i + 1]].tobytes().decode("utf-8", "replace")

    def affiliation(self, i):
        """Main affiliation of author i: (institution name, two-letter country code or "", newest year in which a
        work of the author named it or None), or None when none is known."""
        if not self.has_affiliations:
            return None
        k = int(self.author_institution[i])
        if k < 0:
            return None
        text = self.institutions_blob[self.institutions_off[k]:self.institutions_off[k + 1]].tobytes().decode("utf-8", "replace")
        name, _, country = text.partition("\t")
        year = int(self.author_institution_year[i])
        return name, country, year if year > 0 else None

    def pubs(self, i):
        """Publications of author i, newest first (int32)."""
        pubs = self.author_pubs[self.author_pubs_off[i]:self.author_pubs_off[i + 1]]
        out = self.excluded_pubs.get(i)
        return pubs if out is None else pubs[~np.isin(pubs, out)]

    # ---- publications
    def authors(self, p):
        """Author ids of publication p in the paper's author order (int32)."""
        authors = self.pub_authors[self.pub_authors_off[p]:self.pub_authors_off[p + 1]]
        out = self.excluded_authors.get(p)
        return authors if out is None else authors[~np.isin(authors, out)]

    def authors_count(self, p):
        """Number of authors of publication p known to the graph (authors with an ORCID)."""
        if p in self.excluded_authors:
            return int(self.authors(p).shape[0])
        return int(self.pub_authors_off[p + 1] - self.pub_authors_off[p])

    def year(self, p):
        """Publication year, or None when unknown."""
        y = int(self.pub_year[p])
        return y if y > 0 else None

    def records(self, pubs):
        """The publications as the page lists them, in the given order:
        [{"id": "W...", "pmid": int or None, "doi": str or None, "title": str, "pub_year": int or None}, ...].
        The titles are stored in compressed blocks of title_block publications; a block is decompressed once per call."""
        blocks = {}
        out = []
        for p in pubs:
            b, k = divmod(p, self.title_block)
            records = blocks.get(b)
            if records is None:
                lo, hi = int(self.titles_off[b]), int(self.titles_off[b + 1])
                records = blocks[b] = zlib.decompressobj(-15, self.dictionary).decompress(self.titles_blob[lo:hi].tobytes()).split(b"\n")
            title, _, doi = records[k].partition(b"\t")
            pmid = int(self.pub_pmid[p])
            out.append({"id": "W%d" % int(self.pub_work[p]), "pmid": pmid or None, "doi": doi.decode("utf-8", "replace") or None,
                        "title": title.decode("utf-8", "replace"), "pub_year": self.year(p)})
        return out


def load(folder, exclusions=EXCLUSIONS, hidden=HIDDEN):
    """Graph for the folder, or None when the files are not there."""
    if not folder or not os.path.isfile(os.path.join(folder, "meta.json")):
        return None
    return Graph(folder, exclusions, hidden)
