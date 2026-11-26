"""pubScan WSGI backend: co-authorship networks from the graph files derived from OpenAlex.

pubScan <https://pubscan.org> - Copyright (C) 2025-2026 Gregor Rot
Free software under the GNU Affero General Public License, version 3 (LICENCE.md), with the additional
attribution terms of NOTICE.md: the pubScan notice with its link stays visible in the user interface.

Security notes
- every parameter travels in the query string; request bodies are never read (POST/PUT are rejected)
- actions are dispatched through an explicit table, never through attribute lookup
- all user input is validated/length-capped before it reaches the FTS5 MATCH expression of the name
  search (every token quoted, parameterized query) or the search log (no shell, control chars stripped)
- network builds are limited per client and server-wide so one visitor cannot occupy every worker thread
"""
import html
import json
import math
import os
import sys
import sqlite3
import datetime
import random
import re
import logging
import logging.handlers
import threading
import urllib.parse
from itertools import combinations
from difflib import SequenceMatcher
from unidecode import unidecode
import numpy as np
try:
    from pubscan import graph as graph_module
    from pubscan import topics as topics_module
except ImportError: # running outside the package (tests): same folder
    import graph as graph_module
    import topics as topics_module

# config
max_nodes = 100
max_edges = 1000

def paper_weight(n_authors):
    """Fractional co-authorship: a publication shared by n authors counts 1/(n-1) for each pair of them
    (a two-author paper counts 1, a 10-author paper 0.11, a 300-author consortium paper 0.003), so that
    real collaborators rank above co-signers of large consortium papers."""
    return 1.0 / max(n_authors - 1, 1)

def edge_width(weight):
    """Drawn edge width from the fractional weight: 1 px for consortium-only links, 7.5 px at most."""
    return round(min(7.5, 1.0 + 1.5 * weight), 1)

# request limits
MAX_PARAM_LENGTH = 5000          # longest accepted value of any query-string parameter
MAX_QUERY_FIELDS = 20            # parameters per request
MAX_AUTHOR_LENGTH = 200          # "author" parameter ("Name ORCID")
MAX_PUBS_PER_AUTHOR = 300        # publications kept per author (the newest); a node or edge never carries more than this
MAX_SUGGEST_LENGTH = 100         # "author_name" parameter
MAX_SUGGEST_TOKENS = 8           # name/ORCID tokens used in the FTS prefix query
MAX_TOPIC_AUTHORS = 100          # authors returned for one topic (topic_authors)
MAX_AUTHOR_TOPICS = 100          # terms returned for one author (author_topics)
SUGGEST_NAMES = 6                # name matches in the search box suggestions (4 when a topic matches as well)
SUGGEST_TOPIC_AUTHORS = 5        # authors of the best matching topic in the search box suggestions
MAX_CONCURRENT_NETWORKS = 6      # network builds running at the same time (server-wide)
MAX_CONCURRENT_PER_IP = 2        # network builds running at the same time from one client address

ORCID_RE = re.compile(r"^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$")
CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")

# Dockerfile env vars. pubscan_DB once named the SQLite file with authors and publications; today only its
# folder matters: it is where the data files are looked for
DB_names = os.environ.get("pubscan_DB_names")       # SQLite full-text index of the author names (openalex/db_names.py)
DATA_DIR = os.path.dirname(os.environ.get("pubscan_DB") or DB_names or "")
# memory-mapped graph and title file built by openalex/db_graph.py (default: <data folder>/graph): authors,
# publications, who wrote what, years, ids and titles. Without it no network can be built.
GRAPH_DIR = os.environ.get("pubscan_GRAPH") or (os.path.join(DATA_DIR, "graph") if DATA_DIR else None)
GRAPH = graph_module.load(GRAPH_DIR, os.environ.get("pubscan_EXCLUSIONS") or graph_module.EXCLUSIONS,
                          os.environ.get("pubscan_HIDDEN") or graph_module.HIDDEN)
# author-topic table built by openalex/db_topics.py on top of the graph (default: <data folder>/topics);
# without it the topic search actions answer with an error, everything else is unaffected
TOPICS_DIR = os.environ.get("pubscan_TOPICS") or (os.path.join(DATA_DIR, "topics") if DATA_DIR else None)
TOPICS = topics_module.load(TOPICS_DIR) if GRAPH is not None else None

def sample_edges(edges, count, center_id):
    """Random sample of edges that is the same whenever the same network is requested: the generator is seeded
    with the ORCID of the central author. (A generator seeded once per process would hand every request different
    edges, and with them different clusters, colours and positions in the browser at every reload.)"""
    return random.Random(center_id).sample(edges, count)

def year_span(years):
    """[first, last] of the given publication years (unknown years are skipped), or None when no year is known."""
    known = [year for year in years if isinstance(year, int) and year > 0]
    return [min(known), max(known)] if known else None

def open_db(path):
    c = sqlite3.connect(f"file:{path}?immutable=1", uri=True)
    c.execute("PRAGMA query_only = ON;")
    c.execute("PRAGMA cache_size = 10000;")   # ~10 MB
    c.execute("PRAGMA mmap_size = 268435456;")       # 256 MB memory-mapped I/O
    c.execute("PRAGMA temp_store = MEMORY;")
    c.execute("PRAGMA synchronous = OFF;")
    c.execute("PRAGMA journal_mode = OFF;")
    return c

# one connection per worker thread: a single sqlite3 connection shared by all mod_wsgi threads can
# mix up statements that run at the same time; the file is opened read-only, so per-thread connections are cheap
_thread_db = threading.local()

def get_conn_names():
    c = getattr(_thread_db, "conn_names", None)
    if c is None:
        c = _thread_db.conn_names = open_db(DB_names)
    return c

pubscan_folder = os.path.dirname(os.path.realpath(__file__))

# search log: plain file appends through the logging module (no shell), rotated at 20 MB so that
# request floods cannot fill the disk
log_filename = os.environ.get("pubscan_log_filename") or os.path.join(pubscan_folder, "pubscan.log")
search_log = logging.getLogger("pubscan.search")
search_log.setLevel(logging.INFO)
search_log.propagate = False
if not search_log.handlers:
    try:
        _handler = logging.handlers.RotatingFileHandler(log_filename, maxBytes=20 * 1024 * 1024, backupCount=3, encoding="utf-8")
    except OSError:
        _handler = logging.StreamHandler(sys.stderr)
    _handler.setFormatter(logging.Formatter("%(message)s"))
    search_log.addHandler(_handler)

class BadRequest(Exception):
    pass

class ConcurrencyLimiter:
    """Counts network builds in flight, server-wide and per client address (the WSGI daemon runs
    as one process, so in-memory counters cover every worker thread)."""

    def __init__(self, max_total, max_per_ip):
        self.max_total = max_total
        self.max_per_ip = max_per_ip
        self.lock = threading.Lock()
        self.total = 0
        self.per_ip = {}

    def acquire(self, ip):
        with self.lock:
            if self.total >= self.max_total or self.per_ip.get(ip, 0) >= self.max_per_ip:
                return False
            self.total += 1
            self.per_ip[ip] = self.per_ip.get(ip, 0) + 1
            return True

    def release(self, ip):
        with self.lock:
            self.total = max(0, self.total - 1)
            left = self.per_ip.get(ip, 0) - 1
            if left <= 0:
                self.per_ip.pop(ip, None)
            else:
                self.per_ip[ip] = left

network_limiter = ConcurrencyLimiter(MAX_CONCURRENT_NETWORKS, MAX_CONCURRENT_PER_IP)

def client_ip(environ):
    """Client address for logs and limits. The host reverse proxy appends the real address as the
    last X-Forwarded-For entry (anything before it was sent by the client and is not trusted)."""
    forwarded = environ.get("HTTP_X_FORWARDED_FOR", "")
    ip = forwarded.rsplit(",", 1)[-1] if forwarded else environ.get("REMOTE_ADDR", "")
    ip = re.sub(r"[^0-9a-fA-F.:]", "", ip.strip())[:45]
    return ip or "unknown"

def clean_for_log(text, limit=300):
    return CONTROL_RE.sub(" ", str(text))[:limit]

def sanitize_value(value: str) -> str:
    value = value.strip()
    return re.sub(r'[^\w\s,-]', '', value)

def build_match_query(author_name: str):
    """FTS5 MATCH expression for the autocomplete: every token is a quoted string, so no FTS
    operator, column filter or NEAR syntax can be injected. Tokens of 2+ characters are prefix
    terms (covered by the prefix index), single characters (initials) are exact terms, which keeps
    the query cheap. Returns the expression and the tokens used."""
    tokens = re.findall(r"[a-z0-9]+", unidecode(author_name).lower())[:MAX_SUGGEST_TOKENS]
    query = " ".join(f'"{token}"*' if len(token) > 1 else f'"{token}"' for token in tokens)
    return query, tokens

def name_sort(name, search):
    """Rank a suggestion ("Name ORCID") against the lowercase typed text: exact name first, then names
    starting with the text, then by similarity."""
    person = name.rsplit(" ", 1)[0].lower()
    if person == search:
        return (0, 0)
    elif person.startswith(search):
        return (1, 0)
    else:
        similarity = SequenceMatcher(None, search, person).ratio()
        return (2, -similarity)

class TableClass():

    # action -> (method, JSON response); the only attributes a request can reach
    ACTIONS = {
        "version": ("version", False),
        "get_update_date": ("get_update_date", False),
        "get_author_network": ("get_author_network", True),
        "get_author_publications": ("get_author_publications", True),
        "author_suggest": ("author_suggest", True),
        "suggest": ("suggest", True),
        "topic_suggest": ("topic_suggest", True),
        "topic_authors": ("topic_authors", True),
        "author_topics": ("author_topics", True),
        "page": ("page", "html"),
    }

    def return_string(self, cont):
        return cont.encode("utf-8")

    def log(self, message):
        print(message, file=self.environ['wsgi.errors'])

    def logme(self, message):
        search_log.info(f"{datetime.datetime.now()} [{self.ip}]: {clean_for_log(message)}")

    def __init__(self, environ, start_response):
        self.environ = environ
        self.start = start_response
        self.ip = client_ip(environ)
        self.pars = self.parse_fields(environ)

    def parse_fields(self, environ):
        """Parameters come from the query string only; request bodies are never read.
        The first value of a repeated parameter wins; values are length-capped."""
        pars = {}
        try:
            parsed = urllib.parse.parse_qs(environ.get("QUERY_STRING", ""), max_num_fields=MAX_QUERY_FIELDS)
        except ValueError: # more than MAX_QUERY_FIELDS parameters
            return pars
        for key, values in parsed.items():
            if values:
                pars[key] = values[0][:MAX_PARAM_LENGTH]
        return pars

    def error(self, status, message, extra_headers=()):
        """Error response as one JSON line (the frontend shows "instruction": "error" lines in its log)."""
        headers = [('Content-type', 'application/json; charset=utf-8'), ('X-Content-Type-Options', 'nosniff')] + list(extra_headers)
        self.start(status, headers)
        yield self.return_string(json.dumps({"instruction": "error", "description": f"Error: {message}"}) + "\n")

    def __iter__(self):
        method = self.environ.get("REQUEST_METHOD", "GET")
        if method not in ("GET", "HEAD"):
            yield from self.error("405 Method Not Allowed", "method not allowed, use GET", [("Allow", "GET, HEAD")])
            return

        action = self.pars.get("action", "version")
        spec = self.ACTIONS.get(action)
        if spec is None:
            yield from self.error("404 Not Found", "requested action not found")
            return
        handler_name, is_json = spec
        content_type = 'text/html; charset=utf-8' if is_json == "html" else 'application/json; charset=utf-8' if is_json else 'text/plain; charset=utf-8'
        headers = [('Content-type', content_type), ('X-Content-Type-Options', 'nosniff')]
        if is_json == "html":
            headers.append(('Cache-Control', 'no-cache'))   # as for the static page: browsers ask before reusing a copy

        if method == "HEAD": # headers only, no work
            self.start("200 OK", headers)
            return

        slot = False
        try:
            if action == "get_author_network":
                slot = network_limiter.acquire(self.ip)
                if not slot:
                    yield from self.error("503 Service Unavailable", "the server is busy, please retry in a few seconds", [("Retry-After", "5")])
                    return
            try:
                body = getattr(self, handler_name)() # validates the parameters, returns the response iterable
            except BadRequest as e:
                yield from self.error("400 Bad Request", str(e))
                return
            self.start("200 OK", headers)
            yield from body
        finally:
            if slot: # also runs when the client disconnects (the generator is closed)
                network_limiter.release(self.ip)

    def version(self):
        status_string = f"""pubscan v3.5 {datetime.datetime.now()}
"""
        return [self.return_string(status_string)]

    def get_update_date(self):
        """When the publications were taken from the OpenAlex snapshot (shown in the page header)."""
        try:
            update_str = GRAPH.meta["source_mtime"][:16]      # "YYYY-MM-DD HH:MM"
            yield self.return_string(update_str + "h\n")
        except Exception as e:
            self.logme(f"error reading the data date: {e}")
            yield self.return_string("\n")

    def parse_year(self, key):
        # optional publication year filter (inclusive); only 4-digit values are accepted, anything else = no bound
        val = sanitize_value(self.pars.get(key, "") or "")
        return int(val) if len(val)==4 and val.isdigit() else None

    def get_author_network(self):
        if GRAPH is not None:
            GRAPH.refresh_exclusions() # openalex/exclusions.tab: a new line takes effect without a restart
        query = self.pars.get("author", "")
        if len(query) > MAX_AUTHOR_LENGTH:
            raise BadRequest(f"author parameter longer than {MAX_AUTHOR_LENGTH} characters")
        search = sanitize_value(unidecode(query).lower())
        tokens = search.split()
        orcid = tokens[-1].upper() if tokens else "" # ORCID check digit can be "X"; the search string was lowercased above
        if not ORCID_RE.match(orcid):
            orcid = None # not an ORCID: empty network without touching the database
        year_from = self.parse_year("year_from")
        year_to = self.parse_year("year_to")
        if year_from is not None and year_to is not None and year_from > year_to:
            year_from, year_to = year_to, year_from
        self.logme(f"pubscan search: {search}")
        if GRAPH is None:
            raise BadRequest("the publication data is not available on this server")
        return self.author_network(search, orcid, year_from, year_to)

    def author_network(self, search, center_id, year_from, year_to):
        """Co-authorship network of one author from the memory-mapped graph: publication lists are int32
        slices (newest first), the author lists of a publication are id slices in the paper's order, names
        come from the name blob and years from the per-publication year array."""
        G = GRAPH
        year_filter = year_from is not None or year_to is not None
        def year_ok(year):
            if not isinstance(year, int):
                return False
            if year_from is not None and year < year_from:
                return False
            if year_to is not None and year > year_to:
                return False
            return True
        year_label = f"{year_from or ''}-{year_to or ''}"

        def progress(description):
            return self.return_string(json.dumps({"instruction": "progress", "description": description}) + "\n")

        yield progress(f"looking up the publications of {search}")

        center = G.lookup(center_id) if center_id is not None else None
        if center is None:
            if center_id is None:
                yield progress("the search must end with an ORCID iD (select an author from the suggestions)")
            else:
                yield progress(f"found 0 publications for {center_id}")
            yield self.return_string(json.dumps({"nodes_all": [], "edges_all": [], "instruction": "data"}) + "\n")
            return

        center_id = G.orcid(center) # stored spelling of the ORCID becomes the center id
        center_pubs = G.pubs(center)[:MAX_PUBS_PER_AUTHOR].tolist()
        center_years = year_span(G.year(pub) for pub in center_pubs) # the years a year filter can choose from (ends of the year slider)
        if not year_filter:
            center_pubs = center_pubs[:200] # maximum 200 publications for center

        yield progress(f"found {len(center_pubs)} publications for {center_id}")

        capped = {}   # author id -> the author's newest publications (int32 array, newest first, at most MAX_PUBS_PER_AUTHOR)
        order = []    # author ids in order of first appearance
        center_kept = []
        for index, pub in enumerate(center_pubs):
            if len(center_kept) >= 200:
                break
            if index % 20 == 0:
                yield progress(f"obtaining the authors of publication {index + 1} of {len(center_pubs)} ({int(index/len(center_pubs)*100)} % done)")
            if year_filter and not year_ok(G.year(pub)):
                continue
            center_kept.append(pub)
            for aid in G.authors(pub).tolist():
                if aid not in capped:
                    capped[aid] = G.pubs(aid)[:MAX_PUBS_PER_AUTHOR]
                    order.append(aid)

        group_len = {} # the group colour is assigned from the unfiltered list, n from the filtered one
        if year_filter:
            yield progress(f"year filter {year_label}: {len(center_kept)} of {len(center_pubs)} center publications kept")
            if center in capped:
                group_len[center] = len(capped[center])
                capped[center] = np.array(center_kept, dtype=np.int32)

        authors = [aid for aid in order if len(capped[aid]) >= 1]

        yield progress(f"generating network with {len(authors)} authors")

        weights = {}         # publication -> its fractional weight, looked up once
        def pair_weight(common):
            """Fractional weight of a set of shared publications. math.fsum is exact, so the result does not
            depend on the order of the publications (equal weights stay equal, whatever the numbering)."""
            for p in common:
                if p not in weights:
                    weights[p] = paper_weight(G.authors_count(p))
            return math.fsum(weights[p] for p in common)

        # shared publications of every candidate with the center: a candidate of a large collaboration paper has
        # thousands of fellow candidates, so this step works on the arrays and builds no Python sets
        center_sorted = np.sort(capped[center]) if center in capped else np.zeros(0, dtype=np.int32)
        nodes_cdegree = {}   # shared publications with the center
        nodes_weight = {}    # their fractional weight (see paper_weight)
        for aid in authors:
            if aid == center:
                continue
            common = np.intersect1d(center_sorted, capped[aid], assume_unique=True)
            if common.shape[0]:
                nodes_cdegree[aid] = int(common.shape[0])
                nodes_weight[aid] = pair_weight(common.tolist())

        yield progress("sorting nodes by co-authorship weight with center (shared publications, each weighted by 1/(authors-1)) [descending]")
        temp_sorted = sorted(((nodes_weight.get(aid, 0.0), nodes_cdegree.get(aid, 0), aid) for aid in authors if aid != center),
                             key=lambda x: (x[0], x[1]), reverse=True)   # stable: ties keep first-appearance order

        yield progress(f"keeping {max_nodes} most connected nodes to center")
        kept = [center] if center in capped and len(capped[center]) >= 1 else []
        for weight, degree, aid in temp_sorted:
            if len(kept) < max_nodes and degree > 0:
                kept.append(aid)
        author_sets = {aid: set(capped[aid].tolist()) for aid in kept}   # the pairwise step only concerns the kept nodes

        def node_rec(aid):
            n = len(capped[aid])
            g = group_len.get(aid, n)
            group = "g3" if g > 100 else "g2" if g > 50 else "g1" if g > 10 else "g0"
            rec = {"id": G.orcid(aid), "label": G.name(aid), "group": group, "size": 20, "n": n}
            affiliation = G.affiliation(aid)   # main affiliation (db_affiliations.py): shown with the publication list
            if affiliation:
                rec["aff"], rec["cc"], rec["aff_year"] = affiliation
            return rec
        nodes_all = [node_rec(aid) for aid in kept]

        pair_common = []
        for a1, a2 in combinations(kept, 2):
            common = author_sets[a1] & author_sets[a2]
            if len(common) > 0:
                pair_common.append((a1, a2, common))

        if year_filter and pair_common:
            yield progress(f"year filter {year_label}: applying it to the co-author publications")
            filtered = []
            for a1, a2, common in pair_common:
                common = {p for p in common if year_ok(G.year(p))}
                if common:
                    filtered.append((a1, a2, common))
            pair_common = filtered

        edges_all = []
        for a1, a2, common in pair_common:
            num_common = len(common)
            weight = pair_weight(common)
            edges_all.append({"from": G.orcid(a1), "to": G.orcid(a2), "width": edge_width(weight), "label": f"{num_common}", "common": num_common, "weight": round(weight, 3)})

        yield progress("keeping all edges to centre + 100 others most heavy + sampling randomly others to reach max edges")

        if len(edges_all) > max_edges:
            edges_A = [e for e in edges_all if center_id in (e["from"], e["to"])]
            edges_rest = [e for e in edges_all if center_id not in (e["from"], e["to"])]
            edges_rest.sort(key=lambda x: (x["weight"], x["common"]), reverse=True)
            edges_B = edges_rest[:100]
            edges_all = edges_A + edges_B
            edges_C = sample_edges(edges_rest[100:], max(max_edges - len(edges_all), 0), center_id)
            edges_all = edges_all + edges_C

        yield progress("network construction on server complete, sending data over")
        yield self.return_string(json.dumps({"nodes_all": nodes_all, "edges_all": edges_all, "years": center_years, "instruction": "data"}) + "\n")
        yield progress(f"{len(nodes_all)} author nodes and {len(edges_all)} co-author edges")

    def get_author_publications(self):
        """Publications of one author (author=ORCID) or shared by two (author + coauthor), with the
        optional year filter. Called when a node or an edge is clicked, so the network response does
        not have to carry every publication list."""
        if GRAPH is not None:
            GRAPH.refresh_exclusions()
        author = self.parse_orcid("author")
        coauthor = self.parse_orcid("coauthor") if self.pars.get("coauthor", "") else None
        year_from = self.parse_year("year_from")
        year_to = self.parse_year("year_to")
        if year_from is not None and year_to is not None and year_from > year_to:
            year_from, year_to = year_to, year_from
        return self.author_publications_body(author, coauthor, year_from, year_to)

    def parse_orcid(self, key):
        value = sanitize_value(unidecode(self.pars.get(key, "")[:MAX_AUTHOR_LENGTH])).upper()
        if not ORCID_RE.match(value):
            raise BadRequest(f"{key} must be an ORCID iD")
        return value

    def capped_pubs(self, orcid):
        """The newest MAX_PUBS_PER_AUTHOR publications of an author (internal numbers, newest first)."""
        aid = GRAPH.lookup(orcid)
        return GRAPH.pubs(aid)[:MAX_PUBS_PER_AUTHOR].tolist() if aid is not None else []

    def author_publications_body(self, author, coauthor, year_from, year_to):
        """One JSON line: the list of publications, newest first, each {"id": OpenAlex work id, "pmid", "doi",
        "title", "pub_year"} (pmid and doi are null when the publication has none; a DOI is only stored for
        publications without a PubMed id, whose link it is)."""
        if GRAPH is None:
            raise BadRequest("the publication data is not available on this server")
        pubs = self.capped_pubs(author) # newest first, at most MAX_PUBS_PER_AUTHOR
        if coauthor is not None:
            other = set(self.capped_pubs(coauthor))
            pubs = [p for p in pubs if p in other] # same intersection as the edge weight in the network
        if year_from is not None or year_to is not None:
            def inside(year):
                if year is None:
                    return False
                if year_from is not None and year < year_from:
                    return False
                if year_to is not None and year > year_to:
                    return False
                return True
            pubs = [p for p in pubs if inside(GRAPH.year(p))]
            if coauthor is None: # the center node of a filtered network keeps at most 200 publications
                pubs = pubs[:200]
        yield self.return_string(json.dumps(GRAPH.records(pubs))+"\n")

    # ---- the page itself, for the address of an author or a topic

    def page(self):
        """web/index.html with a head of its own for the address of an author (/<Name>_<ORCID>, /<ORCID>) or a
        topic (/topic/<Name>): title, description, canonical address and the Open Graph fields name the author or
        the topic, so that search engines and link previews see different pages instead of one page under millions
        of addresses. The web server sends these addresses here (path=<the address path>); everything the browser
        shows still comes from the scripts of the page. An address that names nobody is answered with the plain
        page, marked noindex."""
        path = urllib.parse.unquote(self.pars.get("path", ""))[:MAX_AUTHOR_LENGTH + 40].strip("/")
        head = None
        if path == "":   # the start page
            head = {"url": SITE_URL + "/"}
        elif path.startswith("topic/"):
            name = path[len("topic/"):].replace("_", " ").strip()
            terms = TOPICS.suggest(name, limit=20, max_tokens=MAX_SUGGEST_TOKENS) if TOPICS is not None and name else []
            term = next((t for t in terms if t.get("exact")), None)
            if term:
                where = {"topic": "topic in " + (term["subfield"] or term["field"] or "OpenAlex"),
                         "subfield": "subfield of " + (term["field"] or "OpenAlex")}.get(term["kind"], "keyword")
                head = {"title": f"{term['name']}: top authors and researchers | pubScan",
                        "description": f"Top researchers in {term['name']} ({where}): the authors publishing most on the topic, ranked by number of "
                                       f"publications, out of {term['n_authors']:,} researchers and {term['n_pubs']:,} publications. Open their co-authorship networks on pubScan.",
                        "url": SITE_URL + "/topic/" + urllib.parse.quote(term["name"].replace(" ", "_"), safe="!*'()")}
        elif GRAPH is not None:
            orcid = path.rsplit("_", 1)[-1].upper()
            aid = GRAPH.lookup(orcid) if ORCID_RE.match(orcid) else None
            if aid is not None:
                GRAPH.refresh_exclusions()
                n = int(GRAPH.pubs(aid).shape[0])
                if n > 0 and aid not in GRAPH.hidden:
                    name = GRAPH.name(aid)
                    affiliation = GRAPH.affiliation(aid)
                    place = f" ({affiliation[0]})" if affiliation else ""
                    head = {"title": f"{name}: co-authorship network | pubScan",
                            "description": f"The research network of {name}{place}: {n:,} publication{'s' if n != 1 else ''}, co-authors and collaborators, "
                                           f"with their clusters, topics and joint papers. An interactive co-authorship network from OpenAlex and ORCID data, on pubScan.",
                            "url": SITE_URL + "/" + urllib.parse.quote(f"{name} {GRAPH.orcid(aid)}".replace(" ", "_"), safe="!*'()")}
        return [self.return_string(page_html(head))]

    def author_suggest(self):
        author_name = self.pars.get("author_name", "")[:MAX_SUGGEST_LENGTH]
        query, tokens = build_match_query(author_name)
        return self.suggest_body(query, " ".join(tokens))

    def suggest_body(self, query, author_name):
        yield self.return_string(json.dumps(self.author_suggestions(query, author_name)))

    def author_suggestions(self, query, author_name):
        """Author names ("Name ORCID") matching the FTS prefix query, most promising first. The names as they are
        shown are searched first. When they give fewer than 20, the other spellings OpenAlex knows for an author
        (column alt of the index: "andrej" for the author shown as "A. Seljak") fill the list, behind the direct
        matches."""
        def best(names):
            names = sorted(names, key=lambda name: name_sort(name, author_name))[:20] # choose 20 most promising ones
            return sorted(names, key=len) # shorter first
        direct, others = [], []
        hidden = GRAPH.hidden_orcids if GRAPH is not None else ()   # openalex/hidden_authors.tab
        def shown_names(rows):
            names = [row[0] for row in rows]
            return [name for name in names if name[-19:].upper() not in hidden] if hidden else names
        if query:
            try:
                direct = shown_names(get_conn_names().execute("SELECT name FROM names WHERE name MATCH ? LIMIT 50", (query,)))
            except sqlite3.OperationalError:
                direct = []
            if len(direct) < 20:
                try:   # every column of the index: the name and the other spellings
                    shown = set(direct)
                    others = [name for name in shown_names(get_conn_names().execute("SELECT name FROM names WHERE names MATCH ? LIMIT 50", (query,)))
                              if name not in shown]
                except sqlite3.OperationalError:
                    others = []
        return (best(direct) + best(others))[:20]

    def suggest(self):
        """Autocomplete of the search box: author names matching the typed text, and the authors publishing
        most on the best matching topic/keyword: {"authors": ["Name ORCID", ...], "topic": term or null,
        "topic_authors": [{"orcid", "name", "n", "year"}, ...]} (no topic when the topic table is absent)."""
        text = self.pars.get("q", "")[:MAX_SUGGEST_LENGTH]
        if GRAPH is not None:
            GRAPH.refresh_exclusions()
        query, tokens = build_match_query(text)
        authors = self.author_suggestions(query, " ".join(tokens))[:SUGGEST_NAMES]
        topic, topic_authors = None, []
        if TOPICS is not None:
            terms = TOPICS.suggest(text, limit=1, max_tokens=MAX_SUGGEST_TOKENS)
            if terms:
                topic = terms[0]
                authors = authors[:4]
                ids, counts, years = self.without_hidden(*TOPICS.authors(topic["term"]), SUGGEST_TOPIC_AUTHORS)
                topic_authors = [{"orcid": GRAPH.orcid(a), "name": GRAPH.name(a), "n": int(c), "year": int(y) if y > 0 else None}
                                 for a, c, y in zip(ids[:SUGGEST_TOPIC_AUTHORS].tolist(), counts[:SUGGEST_TOPIC_AUTHORS].tolist(), years[:SUGGEST_TOPIC_AUTHORS].tolist())]
        return [self.return_string(json.dumps({"authors": authors, "topic": topic, "topic_authors": topic_authors}))]

    # ---- topic search: OpenAlex topics / subfields / keywords -> authors (openalex/db_topics.py)

    def without_hidden(self, ids, counts, years, limit=None):
        """The authors of a term without the hidden ones (openalex/hidden_authors.tab). With a limit only the
        start of the list is looked at: enough to fill the limit after leaving the hidden authors out."""
        hidden = GRAPH.hidden
        if not hidden:
            return ids, counts, years
        if limit is not None:
            n = limit + len(hidden)
            ids, counts, years = ids[:n], counts[:n], years[:n]
        keep = ~np.isin(ids, np.fromiter(hidden, dtype=np.int64, count=len(hidden)))
        return ids[keep], counts[keep], years[keep]

    def topic_suggest(self):
        """Terms matching the typed text (word prefixes), for the topic autocomplete."""
        if TOPICS is None:
            raise BadRequest("topic search is not available on this server")
        text = self.pars.get("topic", "")[:MAX_SUGGEST_LENGTH]
        return [self.return_string(json.dumps(TOPICS.suggest(text, limit=20, max_tokens=MAX_SUGGEST_TOKENS)))]

    def topic_authors(self):
        """Authors publishing on one term (term=<id> from topic_suggest), most publications first:
        {"term": {...}, "authors": [{"orcid", "name", "n", "year"}, ...]}. year_from keeps only authors
        whose newest publication on the term is from that year or later."""
        if TOPICS is None:
            raise BadRequest("topic search is not available on this server")
        value = self.pars.get("term", "")
        if not value.isdigit() or not TOPICS.valid_term(int(value)):
            raise BadRequest("term must be a term id from topic_suggest")
        term = int(value)
        year_from = self.parse_year("year_from")
        self.logme(f"pubscan topic: {TOPICS.terms[term]['name']} ({term}) year_from={year_from}")
        GRAPH.refresh_exclusions()
        ids, counts, years = self.without_hidden(*TOPICS.authors(term))
        if year_from is not None:
            keep = years >= year_from                 # keeps the count order (boolean mask)
            ids, counts, years = ids[keep], counts[keep], years[keep]
        n_total = int(ids.shape[0])
        ids, counts, years = ids[:MAX_TOPIC_AUTHORS].tolist(), counts[:MAX_TOPIC_AUTHORS].tolist(), years[:MAX_TOPIC_AUTHORS].tolist()
        authors = [{"orcid": GRAPH.orcid(a), "name": GRAPH.name(a), "n": int(c), "year": int(y) if y > 0 else None}
                   for a, c, y in zip(ids, counts, years)]
        return [self.return_string(json.dumps({"term": TOPICS.term_record(term), "n_total": n_total, "authors": authors}) + "\n")]

    def author_topics(self):
        """The terms (topics, subfields, keywords) an author publishes on, most publications first:
        {"author": {"orcid", "name"}, "n_total", "topics": [{"term", "name", "kind", "subfield", "n", "year", "rank"}, ...]}.
        rank is the author's position among the authors of the term when within the first MAX_TOPIC_AUTHORS, else null.
        kind=topic|subfield|keyword restricts the kind; year_from keeps terms with a publication from that year on."""
        if TOPICS is None or not TOPICS.has_author_layout:
            raise BadRequest("topics per author are not available on this server")
        orcid = self.parse_orcid("author")
        kind = self.pars.get("kind", "")
        if kind not in ("", "topic", "subfield", "keyword"):
            raise BadRequest("kind must be topic, subfield or keyword")
        year_from = self.parse_year("year_from")
        aid = GRAPH.lookup(orcid)
        if aid is None or aid in GRAPH.hidden:
            return [self.return_string(json.dumps({"author": {"orcid": orcid, "name": ""}, "n_total": 0, "topics": []}) + "\n")]
        ids, counts, years = TOPICS.author_terms_of(aid)
        if year_from is not None:
            keep = years >= year_from
            ids, counts, years = ids[keep], counts[keep], years[keep]
        result = []
        for t, c, y in zip(ids.tolist(), counts.tolist(), years.tolist()):
            rec = TOPICS.terms[t]
            if kind and rec["kind"] != kind:
                continue
            result.append({"term": t, "name": rec["name"], "kind": rec["kind"], "subfield": rec["subfield"], "n": int(c), "year": int(y) if y > 0 else None})
        result.sort(key=lambda r: (-r["n"], -(r["year"] or 0)))   # most publications first, newest first among equals
        n_total = len(result)
        result = result[:MAX_AUTHOR_TOPICS]
        for rec in result:   # is the author among the top authors of the term (the list topic_authors shows)?
            lo = int(TOPICS.term_off[rec["term"]]); hi = min(lo + MAX_TOPIC_AUTHORS, int(TOPICS.term_off[rec["term"] + 1]))
            hit = np.flatnonzero(TOPICS.term_authors[lo:hi] == aid)
            rec["rank"] = int(hit[0]) + 1 if hit.size else None
        return [self.return_string(json.dumps({"author": {"orcid": GRAPH.orcid(aid), "name": GRAPH.name(aid)}, "n_total": n_total, "topics": result}) + "\n")]

# ---- web/index.html with another head (action page)

SITE_URL = "https://pubscan.org"
PAGE_FILE = os.path.join(os.path.dirname(pubscan_folder), "web", "index.html")
_page_cache = (None, "")     # (modification time and size of the file, its text)


def page_template():
    """The text of web/index.html, read again when the file has changed."""
    global _page_cache
    st = os.stat(PAGE_FILE)
    seen = (st.st_mtime_ns, st.st_size)
    if _page_cache[0] != seen:
        with open(PAGE_FILE, encoding="utf-8") as f:
            _page_cache = (seen, f.read())
    return _page_cache[1]


def page_html(head):
    """The page with title, description, canonical address and Open Graph fields from head ({"title",
    "description", "url"}); with head None the page as it is, but marked noindex (an address that names nobody).
    The values are data (names from OpenAlex and ORCID): they are escaped, and only whole tags of the head that
    the file is known to contain are replaced."""
    text = page_template()
    if head is None:
        return text.replace('<meta name="robots" content="index, follow">', '<meta name="robots" content="noindex, follow">', 1)
    e = {key: html.escape(value, quote=True) for key, value in head.items()}
    text = text.replace("<!-- canonical -->", f'<link rel="canonical" href="{e["url"]}">', 1)
    if "title" not in e:         # the start page: its own title and description
        return text
    for pattern, tag in ((r"<title>[^<]*</title>", f"<title>{e['title']}</title>"),
                         (r'<meta name="description" content="[^"]*">', f'<meta name="description" content="{e["description"]}">'),
                         (r'<meta property="og:title" content="[^"]*">', f'<meta property="og:title" content="{e["title"]}">'),
                         (r'<meta property="og:description" content="[^"]*">', f'<meta property="og:description" content="{e["description"]}">'),
                         (r'<meta property="og:url" content="[^"]*">', f'<meta property="og:url" content="{e["url"]}">')):
        text = re.sub(pattern, lambda m: tag, text, count=1)
    return text


def application(environ, start_response):
    return iter(TableClass(environ, start_response))
