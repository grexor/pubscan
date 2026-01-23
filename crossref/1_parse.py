"""
Parses crossref json objects and creates db.tab.gz with 2 columns (TAB separated):

column 1 = doi
column 2 = comma separated list of orcid
"""

import gzip
import json
import sys
import glob

db_file = gzip.open("db.tab.gz", "wt")

def extract_year(rec):
    for key in ("published-print", "published-online", "published", "issued"):
        date = rec.get(key)
        if date and "date-parts" in date:
            return date["date-parts"][0][0]
    return None

c = 0
json_files = glob.glob("json/*.gz")
for fname in json_files:
    c += 1
    print(f"{fname}, {(c/33402.0)*100:.2f}%")
    with gzip.open(fname, "rt", encoding="utf-8") as f:
        for line in f:
            record = json.loads(line)
            doi = f"https://doi.org/{record['DOI']}"
            authors = record.get("author", None)
            if authors==None:
                #print(record.keys())
                continue
            title = record.get("title", [])
            paper_title = title[0] if title else None
            if paper_title==None:
                continue

            container = record.get("container-title", [])
            journal = container[0] if container else None
            if journal==None:
                continue

            year = extract_year(record)
            if year==None:
                continue

            author_list = []
            for author in authors:
                if "ORCID" in author.keys():
                    author_list.append(author["ORCID"])

            if len(author_list)==0:
                continue

            """
            print(doi)
            print(paper_title)
            print(journal)
            print(year)
            print(author_list)
            """

            db_file.write(f"{doi}\t{','.join(author_list)}\n")

db_file.close()
