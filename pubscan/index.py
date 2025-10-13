import json
import cgi
import urllib
import os
import sys
import sqlite3
import hashlib
import datetime
import random
import pubscan
import glob
import shlex
import copy
import shutil
import time
import yaml
from unidecode import unidecode
import requests
import urllib3
import xmltodict
import pickle
from itertools import combinations
import re
import html
import subprocess
from difflib import SequenceMatcher
urllib3.disable_warnings()
from operator import itemgetter

DB = "/home/gregor/pubscan/parser/pubscan.db"
DB_names = "/home/gregor/pubscan/parser/names.db"

random.seed(42)
conn = sqlite3.connect(f"file:{DB}?immutable=1", uri=True, check_same_thread=False)
conn_names = sqlite3.connect(f"file:{DB_names}?immutable=1", uri=True, check_same_thread=False)

conn.execute("PRAGMA query_only = ON;")
conn.execute("PRAGMA cache_size = 2000000;")       # ~2 GB cache in pages (~2 GB RAM)
conn.execute("PRAGMA mmap_size = 268435456;")       # 256 MB memory-mapped I/O
conn.execute("PRAGMA temp_store = MEMORY;")
conn.execute("PRAGMA synchronous = OFF;")
conn.execute("PRAGMA journal_mode = OFF;")

conn_names.execute("PRAGMA query_only = ON;")
conn_names.execute("PRAGMA cache_size = 2000000;")       # ~2 GB cache in pages (~2 GB RAM)
conn_names.execute("PRAGMA mmap_size = 268435456;")       # 256 MB memory-mapped I/O
conn_names.execute("PRAGMA temp_store = MEMORY;")
conn_names.execute("PRAGMA synchronous = OFF;")
conn_names.execute("PRAGMA journal_mode = OFF;")

def build_like_pattern(author_name: str) -> str:
    tokens = [token.strip() for token in author_name.split() if token.strip()]
    return " ".join(token + "*" for token in tokens)

pubscan_folder = os.path.dirname(os.path.realpath(__file__))
config = yaml.safe_load(open(os.path.join(pubscan_folder, "pubscan.config.yaml")))
log_filename = os.path.join(pubscan_folder, "pubscan.log")

data_folder = os.path.dirname(os.path.realpath(__file__))
data_folder = os.path.join(data_folder, "data")

def sanitize_value(value: str) -> str:
    value = value.strip()
    return re.sub(r'[^\w\s,-]', '', value)
    
def name_sort(name, search):
    if name == search:
        return (0, 0)
    elif name.startswith(search):
        return (1, 0)
    else:
        similarity = SequenceMatcher(None, search, name).ratio()
        return (2, -similarity)

def create_json(results, records="", status=""):
    r = {}
    r["records"] = len(results) if records=="" else records
    r["status"] = status
    data = []
    for result in results:
        data.append(result.get_json())
    r["data"] = data
    return json.dumps(r, default=dthandler)

def normalize_name(name):
    parts = re.split(r'\s+', name.strip())
    if len(parts) > 2:
        parts = [parts[0], parts[-1]]
    return set(parts)

def are_names_equal(name1, name2):
    return normalize_name(name1) == normalize_name(name2)

def get_unique_author_name(db, name):
    for temp in db:
        if are_names_equal(temp, name):
            return temp
    db.add(name)
    return name

def remove_special_characters(text):
    return unidecode(text)

def get_full_name(author):
    if isinstance(author, str):
        return author
    fore_name = unidecode(author.get("ForeName", ""))
    last_name = unidecode(author.get("LastName", ""))
    suffix = unidecode(author.get("Suffix", ""))
    initials = unidecode(author.get("Initials", ""))
    if not fore_name and initials:  
        fore_name = initials
    full_name = f"{fore_name} {last_name}".strip()
    if suffix:
        full_name += f", {suffix}"
    return full_name

class TableClass():

    def return_string(self, cont):
        return cont.encode("utf-8")

    def log(self, message):
        print(message, file=self.environ['wsgi.errors'])

    def logme(self, message):
        client_ip = self.environ.get("HTTP_X_FORWARDED_FOR", self.environ.get("REMOTE_ADDR", "unknown"))
        os.system(f"echo '{datetime.datetime.now()} [{client_ip}]: {message}' >> {log_filename}")        

    def __init__(self, environ, start_response):
        self.environ = environ
        self.start = start_response
        self.get_done = "Get done."
        self.string_remove = "Remove done."
        self.string_put = "Save done."
        self.string_insert = "Save done."
        self.string_save = "Save done."
        self.string_key_conflict = "Key conflict."
        self.pars = self.parse_fields(self.environ)
        self.db = {}

    def __iter__(self):
        status = '200 OK'
        response_type = self.pars.get("response_type", None)
        if response_type in ["plain", None]:
            response_headers = [('Content-type','text/plain; charset=utf-8')]
        elif response_type in ["json"]:
            response_headers = [('Content-type','application/json; charset=utf-8')]
        else:
            response_headers = [('Content-type','text/plain; charset=utf-8')]
        self.stream_out = self.start(status, response_headers)

        try:
            method = getattr(self, sanitize_value(self.pars.get("action", "version")))
            yield from method()
        finally:
            Session.remove()

    def parse_fields(self, environ):
        request_method = environ["REQUEST_METHOD"]
        if environ["REQUEST_METHOD"]=="GET":
            pars = urllib.parse.parse_qs(environ['QUERY_STRING'])
            for par, [val] in pars.items():
                pars[par] = val
        if environ["REQUEST_METHOD"]=="POST":
            self.formdata = cgi.FieldStorage(environ=environ, fp=environ['wsgi.input'])
            pars = {}
            for key in self.formdata.keys():
                  pars[key] = self.formdata[key].value
        return pars

    def version(self):
        status_string = f"""pubscan v1 {datetime.datetime.now()}
"""
        return [self.return_string(status_string)]
      
    def author_pmids(self, author):
        try:
            cur = conn.execute(
                "SELECT author_name, pmids FROM authors WHERE author_name = ?",
                (author,)
            )
            row = cur.fetchone()
        except sqlite3.OperationalError as e:
            self.logme(f"DB error: {e}")
            return author, []

        if row is None:
            return author, []

        # row is either a tuple or (if you kept row_factory) a string/list
        # safer to unpack explicitly
        author_name, pmids = row

        # pmids stored as comma-separated string → turn into sorted list of strings
        pmid_list = [int(x) for x in pmids.split(",") if x.strip().isdigit()]
        pmid_list.sort(reverse=True)
        pmid_list = [str(x) for x in pmid_list]

        return author_name, pmid_list[:300]

    def data_pmid(self, pmid):
        try:
            cur = conn.execute(
                "SELECT pmid, title, pub_year, authors FROM publications WHERE pmid = ?",
                (pmid,)
            )
            row = cur.fetchone()
        except sqlite3.OperationalError as e:
            self.logme(f"DB error: {e}")
            return [], None

        if row is None:
            return [], None

        # unpack row
        pmid_val, title, pub_year, authors_str = row

        authors = [a.strip() for a in authors_str.split(",") if a.strip()]

        # build a dict so you can still access row["title"] etc. like before
        row_dict = {
            "pmid": pmid_val,
            "title": title,
            "pub_year": pub_year,
            "authors": authors_str,
        }

        return authors, row_dict

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

        # Build placeholders (?, ?, ?, ...) for the IN clause
        placeholders = ",".join("?" for _ in pmids)

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

    def get_author_network(self):
        search = sanitize_value(unidecode(self.pars["author"]).lower())
        self.logme(f"pubscan search: {search}")
        authors = set()
        db_authors = set()

        test = {"instruction": "progress", "description": f"download publications PMIDs for {search}"}
        yield self.return_string(json.dumps(test)+"\n")
        sys.stdout.flush()

        center_name, pmids = self.author_pmids(search)
        pmids = pmids[:200] # maximum 200 publications for center

        test = {"instruction": "progress", "description": f"found {len(pmids)} publications for {center_name}"}
        yield self.return_string(json.dumps(test)+"\n")
        sys.stdout.flush()

        author_pmids = {}
        nodes_all = []
        edges_all = []
        publications = {}
        all_pmids = set()
        all_pmids.update(pmids)
        
        for index, pmid in enumerate(pmids):

            test = {"instruction": "progress", "description": f"obtaining authors for PMID {pmid} ({int(index/len(pmids)*100)} % done)"}
            yield self.return_string(json.dumps(test)+"\n")
            sys.stdout.flush()

            pmid_authors, pmid_data = self.data_pmid(pmid)
               
            for author_name in pmid_authors:
                author_name = get_unique_author_name(db_authors, author_name)

                if author_pmids.get(author_name, None)==None:
                    #test = {"instruction": "progress", "description": f"obtaining PMIDs for author {author_name}"}
                    #yield self.return_string(json.dumps(test)+"\n")
                    #sys.stdout.flush()
                    _, author_pmids[author_name] = self.author_pmids(author_name)
                    all_pmids.update(author_pmids[author_name])

                author_group = "g0"
                if len(author_pmids[author_name])>10:
                    author_group = "g1"
                if len(author_pmids[author_name])>50:
                    author_group = "g2"
                if len(author_pmids[author_name])>100:
                    author_group = "g3"
                node_size = min(30, len(author_pmids[author_name]))
                node_size = max(15, node_size)
                node_size = 20
                author_rec = { "id": author_name, "label": author_name, "group": author_group, "size": node_size, "pmids":",".join(author_pmids[author_name])};
                if author_name not in authors:
                    nodes_all.append(author_rec)
                    authors.add(author_name)

        authors = set()
        nodes_all_filtered = []
        for node in nodes_all:
            test = {"instruction": "progress", "description": f"author {node['id']} with {len(author_pmids[node['id']])} PMIDs"}
            yield self.return_string(json.dumps(test)+"\n")
            sys.stdout.flush()
            if len(author_pmids[node["id"]])>=1:
                nodes_all_filtered.append(node)
                authors.add(node["id"])
        nodes_all = nodes_all_filtered
            
        test = {"instruction": "progress", "description": f"generating network with {len(nodes_all)} authors (could take up to 5 sec)"}
        yield self.return_string(json.dumps(test)+"\n")
        sys.stdout.flush()

        nodes_degree = {} # degree of nodes
        nodes_cdegree = {} # degree of node to center node
        author_pairs = list(combinations(authors, 2))
        for a1, a2 in author_pairs:
            p1 = author_pmids[a1]
            p2 = author_pmids[a2]
            common = list(set(p1).intersection(p2))
            if len(common)>0:
                num_common = len(common)
                edge_width = min(num_common, 30)
                edge_rec = {"from":a1, "to":a2, "width": edge_width, "label": f"{num_common}", "common": num_common, "pmids":",".join(common), "color": {"color": '#f5f5f5', "highlight": '#FAA0A0'} }
                nodes_degree[a1] = nodes_degree.get(a1, 0) + num_common
                nodes_degree[a2] = nodes_degree.get(a2, 0) + num_common
                if center_name in [a1, a2]:
                    other_node = a1 if a2==center_name else a2
                    nodes_cdegree[other_node] = num_common
                    nodes_cdegree[center_name] = max(num_common, nodes_cdegree.get(center_name, 0))
                edges_all.append(edge_rec)

        test = {"instruction": "progress", "description": "sorting nodes by common publications with center [desc]"}
        yield self.return_string(json.dumps(test)+"\n")
        sys.stdout.flush()

        # sort nodes by degree
        temp = []
        for node in nodes_all:
            node_id = node["id"]
            dg = nodes_cdegree.get(node_id, 0)
            temp.append((dg, node))
        temp_sorted = sorted(temp, key=lambda x: x[0], reverse=True)

        test = {"instruction": "progress", "description": "keeping 150 most connected nodes to center"}
        yield self.return_string(json.dumps(test)+"\n")
        sys.stdout.flush()

        # filter nodes: <150 nodes, each node must have edges (degree>0)
        authors_all = set()
        nodes_all_filtered = []
        for (degree, node) in temp_sorted:
            if len(nodes_all_filtered)<150 and degree>0:
                nodes_all_filtered.append(node)
                authors_all.add(node["id"])
        nodes_all = nodes_all_filtered

        edges_all_filtered = []
        for edge in edges_all:
            edge_from = edge["from"]
            edge_to = edge["to"]
            num_common = edge["common"]
            if edge_from in authors_all and edge_to in authors_all:
                edges_all_filtered.append(edge)
                nodes_degree[edge_from] = nodes_degree.get(edge_from, 0) + num_common
                nodes_degree[edge_to] = nodes_degree.get(edge_to, 0) + num_common
        edges_all = edges_all_filtered

        test = {"instruction": "progress", "description": "keeping all edges to centre + 100 others most heavy + sampling randomly others to reach max 2000 edges"}
        yield self.return_string(json.dumps(test)+"\n")
        sys.stdout.flush()

        if len(edges_all)>2000:
            edges_A = [] # first, keep all edges that are connected to the center node
            edges_rest = [] # edges not connected to the centre node
            for edge in edges_all:
                if center_name in [edge["from"], edge["to"]]:
                    edges_A.append(edge)
                else:
                    edges_rest.append(edge)
            edges_rest.sort(key=lambda x: x["common"], reverse=True)
            edges_B = edges_rest[:100] # take 100 most connected
            edges_all = edges_A + edges_B
            edges_C = random.sample(edges_rest[100:], max(2000-len(edges_all), 0))
            edges_all = edges_all + edges_C

        test = {"instruction": "progress", "description": "network construction on server complete, sending data over"}
        yield self.return_string(json.dumps(test)+"\n")
        sys.stdout.flush()

        results = {}
        results["nodes_all"] = nodes_all
        results["edges_all"] = edges_all
        results["instruction"] = "data"

        yield self.return_string(json.dumps(results)+"\n")
        sys.stdout.flush()

        test = {"instruction": "progress", "description": f"network of {len(nodes_all)} author nodes and {len(edges_all)} co-author edges"}
        yield self.return_string(json.dumps(test)+"\n")
        sys.stdout.flush()

        #publications = self.data_pmids(list(all_pmids))
        #publications["instruction"] = "pub_data";
        #yield self.return_string(json.dumps(publications)+"\n")
        #sys.stdout.flush()

    def get_publications(self):
        pmids = sanitize_value(self.pars["pmids"])
        pmids = unidecode(pmids).split(",")
        publications = self.data_pmids(list(pmids))
        yield self.return_string(json.dumps(publications)+"\n")
        sys.stdout.flush()

    def author_suggest_grep(self):
        author_name = sanitize_value(self.pars.get("author_name", ""))
        if author_name==None:
            yield self.return_string("error")
            return
        author_name = "( [a-z]+)* ".join(author_name.split(" "))
        result = subprocess.run(f"grep -E '{author_name}' {AUTHOR_FILE}", shell=True, capture_output=True, text=True)
        result = result.stdout.split("\n")
        result = [el for el in result if el!=""]
        result = sorted(result, key=lambda name: name_sort(name, author_name))
        yield self.return_string(json.dumps(result))

    def author_suggest(self):
        author_name = sanitize_value(self.pars.get("author_name", ""))
        if not author_name:
            yield self.return_string("error")
            return
        query = build_like_pattern(author_name)
        try:
            cur = conn_names.execute(
                "SELECT name FROM names WHERE name MATCH ? LIMIT 50",
                (query,)
            )
            result = cur.fetchall()
        except sqlite3.OperationalError:
            #self.logme(f"error")
            result = []
        result = [row[0] for row in result]
        result = sorted(result, key=lambda name: name_sort(name, author_name))[:20] # choose 20 most promising ones
        result = sorted(result, key=len) # shorter first
        yield self.return_string(json.dumps(result))

def application(environ, start_response):
    return iter(TableClass(environ, start_response))
