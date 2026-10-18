"""pubScan WSGI backend: co-authorship networks from the OpenAlex-derived SQLite databases.

Security notes
- every parameter travels in the query string; request bodies are never read (POST/PUT are rejected)
- actions are dispatched through an explicit table, never through attribute lookup
- all user input is validated/length-capped before it reaches SQLite (parameterized queries only),
  the FTS5 MATCH expression (every token quoted) or the search log (no shell, control chars stripped)
- network builds are limited per client and server-wide so one visitor cannot occupy every worker thread
"""
import json
import os
import sys
import sqlite3
import datetime
import random
import gc
import re
import logging
import logging.handlers
import threading
import urllib.parse
from itertools import combinations
from difflib import SequenceMatcher
from unidecode import unidecode
import ctypes
import ctypes.util

def malloc_trim():
    try:
        libc = ctypes.CDLL(ctypes.util.find_library("c"))
        libc.malloc_trim(0)
    except Exception:
        pass

# config
max_nodes = 100
max_edges = 1000

# request limits
MAX_PARAM_LENGTH = 5000          # longest accepted value of any query-string parameter
MAX_QUERY_FIELDS = 20            # parameters per request
MAX_AUTHOR_LENGTH = 200          # "author" parameter ("Name ORCID")
MAX_PMIDS_PER_AUTHOR = 300       # publications kept per author; a node or edge never carries more than this
MAX_SUGGEST_LENGTH = 100         # "author_name" parameter
MAX_SUGGEST_TOKENS = 8           # name/ORCID tokens used in the FTS prefix query
MAX_CONCURRENT_NETWORKS = 6      # network builds running at the same time (server-wide)
MAX_CONCURRENT_PER_IP = 2        # network builds running at the same time from one client address

ORCID_RE = re.compile(r"^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$")
PMID_RE = re.compile(r"^[0-9]{1,12}$")
CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")

# Dockerfile env vars
DB = os.environ.get("pubscan_DB")
DB_names = os.environ.get("pubscan_DB_names")

random.seed(42)
conn = sqlite3.connect(f"file:{DB}?immutable=1", uri=True, check_same_thread=False)
conn_names = sqlite3.connect(f"file:{DB_names}?immutable=1", uri=True, check_same_thread=False)

conn.execute("PRAGMA query_only = ON;")
conn.execute("PRAGMA cache_size = 10000;")   # ~10 MB
conn.execute("PRAGMA mmap_size = 268435456;")       # 256 MB memory-mapped I/O
conn.execute("PRAGMA temp_store = MEMORY;")
conn.execute("PRAGMA synchronous = OFF;")
conn.execute("PRAGMA journal_mode = OFF;")

conn_names.execute("PRAGMA query_only = ON;")
conn_names.execute("PRAGMA cache_size = 10000;")
conn_names.execute("PRAGMA mmap_size = 268435456;")       # 256 MB memory-mapped I/O
conn_names.execute("PRAGMA temp_store = MEMORY;")
conn_names.execute("PRAGMA synchronous = OFF;")
conn_names.execute("PRAGMA journal_mode = OFF;")

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
    if name == search:
        return (0, 0)
    elif name.startswith(search):
        return (1, 0)
    else:
        similarity = SequenceMatcher(None, search, name).ratio()
        return (2, -similarity)

class TableClass():

    # action -> (method, JSON response); the only attributes a request can reach
    ACTIONS = {
        "version": ("version", False),
        "get_update_date": ("get_update_date", False),
        "get_author_network": ("get_author_network", True),
        "get_publications": ("get_publications", True),
        "author_suggest": ("author_suggest", True),
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
        content_type = 'application/json; charset=utf-8' if is_json else 'text/plain; charset=utf-8'
        headers = [('Content-type', content_type), ('X-Content-Type-Options', 'nosniff')]

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
        status_string = f"""pubscan v3.1 {datetime.datetime.now()}
"""
        return [self.return_string(status_string)]

    def author_pmids(self, orcid):
        try:
            cur = conn.execute(
                "SELECT orcid, author_name, pmids FROM authors WHERE orcid = ?",
                (orcid,)
            )
            row = cur.fetchone()
        except sqlite3.OperationalError as e:
            self.logme(f"DB error: {e}")
            return orcid, "", []

        if row is None:
            return orcid, "", []

        orcid, author_name, pmids = row

        # pmids stored as comma-separated string -> sorted list of strings, newest first
        pmid_list = [int(x) for x in pmids.split(",") if x.strip().isdigit()]
        pmid_list.sort(reverse=True)
        pmid_list = [str(x) for x in pmid_list]

        return orcid, author_name, pmid_list[:MAX_PMIDS_PER_AUTHOR]

    def data_pmid(self, pmid):
        try:
            cur = conn.execute(
                "SELECT pmid, title, pub_year, authors_name, authors_orcid FROM publications WHERE pmid = ?",
                (pmid,)
            )
            row = cur.fetchone()
        except sqlite3.OperationalError as e:
            self.logme(f"DB error: {e}")
            return [], [], None

        if row is None:
            return [], [], None

        pmid_val, title, pub_year, authors_str, authors_orcid_str = row

        authors = [a.strip() for a in authors_str.split(",") if a.strip()]
        orcids = [a.strip() for a in authors_orcid_str.split(",") if a.strip()]

        row_dict = {
            "pmid": pmid_val,
            "title": title,
            "pub_year": pub_year,
            "authors": authors_str,
            "orcids": orcids,
        }

        return authors, orcids, row_dict

    def get_update_date(self):
        try:
            ctime = os.path.getctime(DB)
            update_str = datetime.datetime.fromtimestamp(ctime).strftime("%Y-%m-%d %H:%M")
            yield self.return_string(update_str + "h\n")
        except Exception as e:
            self.logme(f"error reading file date: {e}")
            yield self.return_string("\n")

    def data_pmids(self, pmids):
        result = {}

        if not pmids:
            return result

        placeholders = ",".join("?" for _ in pmids) # (?, ?, ?, ...) for the IN clause

        try:
            cur = conn.execute(
                f"SELECT pmid, title, pub_year FROM publications WHERE pmid IN ({placeholders})",
                tuple(pmids)
            )
            rows = cur.fetchall()
        except sqlite3.OperationalError as e:
            self.logme(f"DB error: {e}")
            return result

        for row in rows:
            pmid_val, title, pub_year = row
            result[pmid_val] = {
                "pmid": pmid_val,
                "title": title,
                "pub_year": pub_year,
            }

        return result

    def parse_year(self, key):
        # optional publication year filter (inclusive); only 4-digit values are accepted, anything else = no bound
        val = sanitize_value(self.pars.get(key, "") or "")
        return int(val) if len(val)==4 and val.isdigit() else None

    def get_author_network(self):
        query = self.pars.get("author", "")
        if len(query) > MAX_AUTHOR_LENGTH:
            raise BadRequest(f"author parameter longer than {MAX_AUTHOR_LENGTH} characters")
        search = sanitize_value(unidecode(query).lower())
        tokens = search.split()
        orcid = tokens[-1].upper() if tokens else "" # ORCID check digit can be "X"; the search string was lowercased above, DB stores "X"
        if not ORCID_RE.match(orcid):
            orcid = None # not an ORCID: empty network without touching the database
        year_from = self.parse_year("year_from")
        year_to = self.parse_year("year_to")
        if year_from is not None and year_to is not None and year_from > year_to:
            year_from, year_to = year_to, year_from
        self.logme(f"pubscan search: {search}")
        return self.author_network(search, orcid, year_from, year_to)

    def author_network(self, search, center_id, year_from, year_to):
        authors = set()
        db_authors = {}

        year_filter = year_from is not None or year_to is not None
        def year_ok(year):
            if year is None: # unknown year: cannot be inside the range
                return False
            if year_from is not None and year < year_from:
                return False
            if year_to is not None and year > year_to:
                return False
            return True
        year_label = f"{year_from or ''}-{year_to or ''}"

        def progress(description):
            return self.return_string(json.dumps({"instruction": "progress", "description": description}) + "\n")

        yield progress(f"download publications PMIDs for {search}")

        if center_id is None:
            yield progress("the search must end with an ORCID iD (select an author from the suggestions)")
            yield self.return_string(json.dumps({"nodes_all": [], "edges_all": [], "instruction": "data"}) + "\n")
            return

        _, _, pmids = self.author_pmids(center_id)
        if not year_filter:
            pmids = pmids[:200] # maximum 200 publications for center
        # with a year filter all (up to 300) publications are scanned and the first 200 inside the range are kept

        yield progress(f"found {len(pmids)} publications for {center_id}")

        author_pmids = {}
        nodes_all = []
        edges_all = []
        pmid_year = {} # years of publications looked up so far (center publications + edge publications)
        center_kept = [] # center publications inside the year range (all of them when no filter)

        for index, pmid in enumerate(pmids):

            if len(center_kept) >= 200:
                break

            if index % 20 == 0: # progress every 20th publication; one line per publication floods the client log
                yield progress(f"obtaining authors for PMID {pmid} ({int(index/len(pmids)*100)} % done)")

            pmid_authors, pmid_orcids, pmid_data = self.data_pmid(pmid)
            if pmid_data is None:
                continue
            pmid_year[pmid] = pmid_data["pub_year"]
            if year_filter and not year_ok(pmid_data["pub_year"]):
                continue
            center_kept.append(pmid)
            for author_orcid, author_name in zip(pmid_orcids, pmid_authors):
                if author_pmids.get(author_orcid, None)==None:
                    _, _, author_pmids[author_orcid] = self.author_pmids(author_orcid)

                author_group = "g0"
                if len(author_pmids[author_orcid])>10:
                    author_group = "g1"
                if len(author_pmids[author_orcid])>50:
                    author_group = "g2"
                if len(author_pmids[author_orcid])>100:
                    author_group = "g3"
                node_size = 20
                author_rec = { "id": author_orcid, "label": author_name, "group": author_group, "size": node_size, "pmids":",".join(author_pmids[author_orcid])};
                db_authors[author_orcid] = author_name
                if author_orcid not in authors:
                    nodes_all.append(author_rec)
                    authors.add(author_orcid)

        if year_filter:
            yield progress(f"year filter {year_label}: {len(center_kept)} of {len(pmids)} center publications kept")
            # the center node only carries publications inside the range: degrees to the center and
            # the center's publication list then respect the filter
            author_pmids[center_id] = center_kept
            for node in nodes_all:
                if node["id"] == center_id:
                    node["pmids"] = ",".join(center_kept)

        authors = set()
        nodes_all_filtered = []
        for node in nodes_all:
            if len(author_pmids[node["id"]])>=1:
                nodes_all_filtered.append(node)
                authors.add(node["id"])
        nodes_all = nodes_all_filtered

        yield progress(f"generating network with {len(nodes_all)} authors")

        # PMID sets built once per author (instead of once per pair)
        author_sets = {a: set(author_pmids[a]) for a in authors}

        # degree of each node to the center node: one intersection per author
        # (the all-pairs loop is only needed among the nodes we keep, see below)
        nodes_cdegree = {}
        center_set = author_sets.get(center_id, set())
        for a in authors:
            if a == center_id:
                continue
            num_common = len(center_set & author_sets[a])
            if num_common > 0:
                nodes_cdegree[a] = num_common
                nodes_cdegree[center_id] = max(num_common, nodes_cdegree.get(center_id, 0))

        yield progress("sorting nodes by common publications with center [descending]")

        # sort nodes by degree to center (stable sort: ties keep first-appearance order)
        temp = []
        for node in nodes_all:
            node_id = node["id"]
            dg = nodes_cdegree.get(node_id, 0)
            temp.append((dg, node))
        temp_sorted = sorted(temp, key=lambda x: x[0], reverse=True)

        yield progress(f"keeping {max_nodes} most connected nodes to center")

        # filter nodes: <max_nodes nodes, each node must share a publication with the center (degree>0)
        authors_all = set()
        nodes_all_filtered = []
        for (degree, node) in temp_sorted:
            if len(nodes_all_filtered)<max_nodes and degree>0:
                nodes_all_filtered.append(node)
                authors_all.add(node["id"])
        nodes_all = nodes_all_filtered

        # edges only among kept nodes: all pairs of at most max_nodes nodes
        kept_ids = [node["id"] for node in nodes_all]
        pair_common = []
        for a1, a2 in combinations(kept_ids, 2):
            common = author_sets[a1] & author_sets[a2]
            if len(common)>0:
                pair_common.append((a1, a2, common))

        if year_filter and pair_common:
            # publications shared by two co-authors (not involving the center) have unknown years:
            # look them up in batches, then drop the ones outside the range from every edge
            need = set()
            for _, _, common in pair_common:
                need.update(common)
            need = [p for p in need if p not in pmid_year]
            yield progress(f"year filter {year_label}: looking up years of {len(need)} co-author publications")
            for start in range(0, len(need), 500):
                chunk = need[start:start+500]
                placeholders = ",".join("?" for _ in chunk)
                try:
                    rows = conn.execute(f"SELECT pmid, pub_year FROM publications WHERE pmid IN ({placeholders})", tuple(chunk)).fetchall()
                except sqlite3.OperationalError as e:
                    self.logme(f"DB error: {e}")
                    rows = []
                for pmid_val, pub_year in rows:
                    pmid_year[str(pmid_val)] = pub_year
            filtered = []
            for a1, a2, common in pair_common:
                common = {p for p in common if year_ok(pmid_year.get(p))}
                if common:
                    filtered.append((a1, a2, common))
            pair_common = filtered

        for a1, a2, common in pair_common:
            num_common = len(common)
            edge_width = min(num_common, 15)
            edge_rec = {"from":a1, "to":a2, "from_label":db_authors[a1], "to_label":db_authors[a2], "width": edge_width, "label": f"{num_common}", "common": num_common, "pmids":",".join(common), "color": {"color": '#f5f5f5', "highlight": '#FAA0A0'} }
            edges_all.append(edge_rec)

        yield progress("keeping all edges to centre + 100 others most heavy + sampling randomly others to reach max edges")

        if len(edges_all)>max_edges:
            edges_A = [] # first, keep all edges that are connected to the center node
            edges_rest = [] # edges not connected to the centre node
            for edge in edges_all:
                if center_id in [edge["from"], edge["to"]]:
                    edges_A.append(edge)
                else:
                    edges_rest.append(edge)
            edges_rest.sort(key=lambda x: x["common"], reverse=True)
            edges_B = edges_rest[:100] # take 100 most connected
            edges_all = edges_A + edges_B
            edges_C = random.sample(edges_rest[100:], max(max_edges-len(edges_all), 0))
            edges_all = edges_all + edges_C

        yield progress("network construction on server complete, sending data over")

        results = {}
        results["nodes_all"] = nodes_all
        results["edges_all"] = edges_all
        results["instruction"] = "data"

        yield self.return_string(json.dumps(results)+"\n")

        yield progress(f"{len(nodes_all)} author nodes and {len(edges_all)} co-author edges")

        # explicit cleanup
        author_pmids.clear()
        author_sets.clear()
        gc.collect()
        malloc_trim()

    def get_publications(self):
        raw = self.pars.get("pmids", "")
        pmids = []
        seen = set()
        for token in raw.split(","):
            token = token.strip()
            if PMID_RE.match(token) and token not in seen: # digits only, duplicates dropped
                seen.add(token)
                pmids.append(int(token))
                if len(pmids) >= MAX_PMIDS_PER_AUTHOR: # a node/edge never carries more; the rest is ignored
                    break
        return self.publications_body(pmids)

    def publications_body(self, pmids):
        publications = self.data_pmids(pmids)
        yield self.return_string(json.dumps(publications)+"\n")

    def author_suggest(self):
        author_name = self.pars.get("author_name", "")[:MAX_SUGGEST_LENGTH]
        query, tokens = build_match_query(author_name)
        return self.suggest_body(query, " ".join(tokens))

    def suggest_body(self, query, author_name):
        result = []
        if query:
            try:
                cur = conn_names.execute(
                    "SELECT name FROM names WHERE name MATCH ? LIMIT 50",
                    (query,)
                )
                result = [row[0] for row in cur.fetchall()]
            except sqlite3.OperationalError:
                result = []
        result = sorted(result, key=lambda name: name_sort(name, author_name))[:20] # choose 20 most promising ones
        result = sorted(result, key=len) # shorter first
        yield self.return_string(json.dumps(result))

def application(environ, start_response):
    return iter(TableClass(environ, start_response))
