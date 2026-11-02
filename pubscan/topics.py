"""Author-topic table built by openalex/db_topics.py: who publishes on which OpenAlex topic or keyword.

The per-term author lists are memory-mapped numpy arrays (shared between processes, paged in on first
use); the term vocabulary (a few MB of JSON) is held in memory with a word-prefix index for the
autocomplete. Author ids are positions in the graph's ORCID table, so names come from graph.py.
"""
import os
import re
import json
import bisect
import numpy as np
from unidecode import unidecode

WORD_RE = re.compile(r"[a-z0-9]+")
KIND_RANK = {"topic": 0, "subfield": 1, "keyword": 2}


def words(text):
    return WORD_RE.findall(unidecode(text or "").lower())


class Topics:

    FILES = ("term_off", "term_authors", "term_counts", "term_years")

    def __init__(self, folder):
        self.folder = folder
        for name in self.FILES:
            setattr(self, name, np.load(os.path.join(folder, name + ".npy"), mmap_mode="r"))
        self.meta = json.load(open(os.path.join(folder, "meta.json")))
        self.terms = json.load(open(os.path.join(folder, "terms.json"), encoding="utf-8"))
        self.n_terms = len(self.terms)
        # word -> term ids, and the sorted word list for prefix ranges
        index = {}
        self.term_words = []
        for t, rec in enumerate(self.terms):
            ws = words(rec["name"])
            self.term_words.append(ws)
            for w in set(ws):
                index.setdefault(w, []).append(t)
        self.word_terms = {w: np.array(ts, dtype=np.int32) for w, ts in index.items()}
        self.word_list = sorted(self.word_terms)
        self.exact = {}
        for t, ws in enumerate(self.term_words):
            self.exact.setdefault(" ".join(ws), []).append(t)

    # ---- vocabulary
    def terms_with_prefix(self, token):
        """Ids of the terms with a word starting with token (token itself is a full word or a prefix)."""
        lo = bisect.bisect_left(self.word_list, token)
        hi = bisect.bisect_right(self.word_list, token + "￿")
        if lo == hi:
            return np.zeros(0, dtype=np.int32)
        return np.unique(np.concatenate([self.word_terms[w] for w in self.word_list[lo:hi]]))

    def suggest(self, text, limit=20, max_tokens=8):
        """Terms whose name contains every typed token as a word prefix, best first: exact names, then
        by number of publications (topics before keywords when equal)."""
        tokens = words(text)[:max_tokens]
        if not tokens:
            return []
        hits = None
        for token in tokens:
            ids = self.terms_with_prefix(token)
            hits = ids if hits is None else np.intersect1d(hits, ids, assume_unique=True)
            if hits.shape[0] == 0:
                return []
        typed = " ".join(tokens)
        exact = set(self.exact.get(typed, ()))

        def rank(t):
            rec = self.terms[t]
            return (0 if t in exact else 1, -rec["n_pubs"], KIND_RANK.get(rec["kind"], 9))
        best = sorted(hits.tolist(), key=rank)[:limit]
        return [dict(self.term_record(t), exact=t in exact) for t in best if self.terms[t]["n_authors"] > 0]

    def term_record(self, t):
        rec = self.terms[t]
        return {"term": t, "name": rec["name"], "kind": rec["kind"], "subfield": rec["subfield"], "field": rec["field"],
                "n_authors": rec["n_authors"], "n_pubs": rec["n_pubs"]}

    # ---- authors of a term
    def valid_term(self, t):
        return 0 <= t < self.n_terms

    def authors(self, t):
        """(author ids, publication counts, newest years) of term t, strongest author first (views)."""
        lo, hi = int(self.term_off[t]), int(self.term_off[t + 1])
        return self.term_authors[lo:hi], self.term_counts[lo:hi], self.term_years[lo:hi]


def load(folder):
    """Topics for the folder, or None when the files are not there (topic search is then unavailable)."""
    if not folder or not os.path.isfile(os.path.join(folder, "meta.json")):
        return None
    return Topics(folder)
