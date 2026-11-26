"""Write the sitemap of the site: the addresses search engines are invited to read.

usage: python sitemap.py [graph folder [topics folder [output folder]]]      (defaults: graph topics ../web)

Listed are the start page, the pages /find-collaborators and /topics, the list of every topic and subfield (/topic/<Name>), and the network of the authors
of the TOP authors publishing most on each topic (/<Name>_<ORCID>, the address the page itself
uses). Hidden authors (hidden_authors.tab) are left out. A sitemap file holds at most 50,000 addresses, so the
authors are spread over several files; sitemap.xml is the index of all of them and is what robots.txt names.
"""
import json
import os
import sys
import time
import urllib.parse
from xml.sax.saxutils import escape

import numpy as np

graph_dir = sys.argv[1] if len(sys.argv) > 1 else "graph"
topics_dir = sys.argv[2] if len(sys.argv) > 2 else "topics"
out_dir = sys.argv[3] if len(sys.argv) > 3 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "web")
SITE = "https://pubscan.org"
TOP = 10                   # authors per topic (a network is drawn by scripts, so a search engine sees little on
                           # an author's address: a short list of well-linked authors rather than every author)
PER_FILE = 45_000
HIDDEN = os.environ.get("HIDDEN") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "hidden_authors.tab")

t0 = time.time()
today = time.strftime("%Y-%m-%d")
orcids = np.load(os.path.join(graph_dir, "orcids.npy"), mmap_mode="r")
names_blob = np.load(os.path.join(graph_dir, "names_blob.npy"), mmap_mode="r")
names_off = np.load(os.path.join(graph_dir, "names_off.npy"), mmap_mode="r")
terms = json.load(open(os.path.join(topics_dir, "terms.json"), encoding="utf-8"))
term_off = np.load(os.path.join(topics_dir, "term_off.npy"), mmap_mode="r")
term_authors = np.load(os.path.join(topics_dir, "term_authors.npy"), mmap_mode="r")

hidden = set()
if os.path.exists(HIDDEN):
    for line in open(HIDDEN, encoding="utf-8"):
        cells = line.split("#")[0].split()
        if cells:
            hidden.add(cells[0].rsplit("/", 1)[-1].upper())


def address(*parts):
    """Address of a page: the parts joined by "/", each written as the page itself writes it (a "/" inside a name is %2F)."""
    return SITE + "/" + "/".join(urllib.parse.quote(part.replace(" ", "_"), safe="!*'()") for part in parts)


topic_urls, authors, seen = [], [], set()
for t, rec in enumerate(terms):
    if rec["kind"] not in ("topic", "subfield") or rec["n_authors"] <= 0:
        continue
    topic_urls.append(address("topic", rec["name"]))
    for a in term_authors[int(term_off[t]):int(term_off[t]) + TOP].tolist():   # strongest authors first
        if a not in seen:
            seen.add(a)
            authors.append(a)
author_urls = []
for a in authors:
    orcid = orcids[a].decode("utf-8")
    if orcid.upper() in hidden:
        continue
    name = names_blob[int(names_off[a]):int(names_off[a + 1])].tobytes().decode("utf-8", "replace")
    author_urls.append(address(f"{name} {orcid}"))


def write(name, urls):
    with open(os.path.join(out_dir, name + ".tmp"), "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')
        for url in urls:
            f.write(f"<url><loc>{escape(url)}</loc><lastmod>{today}</lastmod></url>\n")
        f.write("</urlset>\n")
    os.replace(os.path.join(out_dir, name + ".tmp"), os.path.join(out_dir, name))


for old in os.listdir(out_dir):                      # files of an earlier, longer list
    if old.startswith("sitemap_") and old.endswith(".xml"):
        os.remove(os.path.join(out_dir, old))
files = ["sitemap_topics.xml"]
write(files[0], [SITE + "/", SITE + "/find-collaborators", SITE + "/topics"] + topic_urls)
for k in range(0, len(author_urls), PER_FILE):
    files.append(f"sitemap_authors_{k // PER_FILE + 1}.xml")
    write(files[-1], author_urls[k:k + PER_FILE])
with open(os.path.join(out_dir, "sitemap.xml.tmp"), "w", encoding="utf-8") as f:
    f.write('<?xml version="1.0" encoding="UTF-8"?>\n<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')
    for name in files:
        f.write(f"<sitemap><loc>{SITE}/{name}</loc><lastmod>{today}</lastmod></sitemap>\n")
    f.write("</sitemapindex>\n")
os.replace(os.path.join(out_dir, "sitemap.xml.tmp"), os.path.join(out_dir, "sitemap.xml"))
print(f"Sitemap written to {out_dir}: {len(topic_urls):,} topic lists and {len(author_urls):,} author networks in {len(files)} files, {time.time() - t0:.0f}s")
