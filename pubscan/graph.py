"""Memory-mapped co-authorship graph built by openalex/db_graph.py.

All arrays are opened read-only with numpy's mmap, so the operating system shares one copy
between processes, pages load on first use and nothing has to be parsed per request. Every
method returns views or small Python objects; the object is safe to share between threads.
"""
import os
import json
import numpy as np


class Graph:

    FILES = ("orcids", "names_blob", "names_off", "author_pmids", "author_pmids_off",
             "pmid_authors", "pmid_authors_off", "pmid_year")

    def __init__(self, folder):
        self.folder = folder
        for name in self.FILES:
            setattr(self, name, np.load(os.path.join(folder, name + ".npy"), mmap_mode="r"))
        self.meta = json.load(open(os.path.join(folder, "meta.json")))
        self.n_authors = int(self.orcids.shape[0])
        self.max_pmid = int(self.pmid_year.shape[0]) - 1

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

    def pmids(self, i):
        """PMIDs of author i, newest first (int32 view)."""
        return self.author_pmids[self.author_pmids_off[i]:self.author_pmids_off[i + 1]]

    # ---- publications
    def has_publication(self, pmid):
        return 0 <= pmid <= self.max_pmid and self.pmid_year[pmid] >= 0

    def authors(self, pmid):
        """Author ids of a PMID in the paper's author order (int32 view; empty if unknown)."""
        if not (0 <= pmid <= self.max_pmid):
            return self.pmid_authors[0:0]
        return self.pmid_authors[self.pmid_authors_off[pmid]:self.pmid_authors_off[pmid + 1]]

    def year(self, pmid):
        """Publication year or None when unknown / no publications row."""
        if not (0 <= pmid <= self.max_pmid):
            return None
        y = int(self.pmid_year[pmid])
        return y if y > 0 else None


def load(folder):
    """Graph for the folder, or None when the files are not there (the server then uses SQLite)."""
    if not folder or not os.path.isfile(os.path.join(folder, "meta.json")):
        return None
    return Graph(folder)
