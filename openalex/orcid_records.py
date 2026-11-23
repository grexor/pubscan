"""Extract names and publication lists from the ORCID Public Data File (the "summaries" archive that ORCID
publishes once a year, CC0, https://info.orcid.org/documentation/integration-guide/working-with-bulk-data).

usage: python orcid_records.py <ORCID_YYYY_MM_summaries.tar.gz> [output file]      (default output: orcid_records.tab.gz)

The ORCID record is what the person, or a party the person has authorised, states about themselves. pubScan takes
the name from it, as the person entered it: db_graph.py shows the given names and the family name instead of the
author name of OpenAlex, which is often abbreviated ("A. Seljak"), and db_names.py makes the credit name and the
other names searchable. The identifiers of the works a record lists (DOI, PubMed id, arXiv id) are written out as
well; pubScan does not use them, who wrote what is taken from OpenAlex.

Output (tab separated, one line per record that has a public name or a work with an identifier; no quoting, tabs
and line breaks inside a field become spaces):
  orcid        0000-0002-1825-0097
  given        given names, as public in the record
  family       family name
  credit       credit name (the name the person publishes under), if any
  other        other names, joined by "|"
  works        identifiers of the listed works, joined by " ": d:<doi in lower case>  p:<PubMed id>
               (an arXiv id is written as the DOI that arXiv registers for it, d:10.48550/arxiv.<id>)

Only identifiers with the relationship "self" or "version-of" count: "part-of" names the book or journal that
contains the work, "funded-by" a grant. The archive (about 50 GB, 900 GB of XML in 20 million files) is read as a
stream: it is never unpacked. gunzip runs in a process of its own; the unpacked bytes are cut where a record
begins (every record starts with the XML declaration; the tar headers between the records are skipped) and
the pieces are searched by WORKERS processes with plain byte searches, which is what makes an hour suffice.
"""
import gzip
import html
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import multiprocessing as mp

archive = sys.argv[1]
output = sys.argv[2] if len(sys.argv) > 2 else "orcid_records.tab.gz"
WORKERS = int(os.environ.get("WORKERS", "4"))
CHUNK = 32 << 20                  # bytes of the unpacked stream handed to a worker at once

RECORD = b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'      # every record of the archive starts with this
DOI = re.compile(r"10\.\d{4,9}/\S+")
ARXIV = re.compile(r"(?:arxiv:)?\s*((?:\d{4}\.\d{4,5})|(?:[a-z\-]+(?:\.[a-z]{2})?/\d{7}))(?:v\d+)?$")


def between(data, opening, closing, start=0, end=None):
    """Bytes between an opening and the next closing tag (plain search, no pattern matching), or None."""
    i = data.find(opening, start, end)
    if i < 0:
        return None
    i += len(opening)
    j = data.find(closing, i, end)
    return data[i:j] if j >= 0 else None


def text(value):
    """Content of an XML element as one clean line."""
    if not value:
        return ""
    value = value.decode("utf-8", "replace")
    if "&" in value:
        value = html.unescape(value)
    return " ".join(value.replace("|", " ").split())


def identifiers(works):
    """The identifiers of the works section: {"d:10.1/x", "p:123", ...}."""
    found = set()
    for block in works.split(b"<common:external-id>")[1:]:
        block = block[:block.find(b"</common:external-id>")]
        kind = (between(block, b"<common:external-id-type>", b"</common:external-id-type>") or b"").strip().lower()
        if kind not in (b"doi", b"pmid", b"arxiv"):
            continue
        if (between(block, b"<common:external-id-relationship>", b"</common:external-id-relationship>") or b"").strip() not in (b"self", b"version-of"):
            continue
        i = block.find(b"<common:external-id-normalized")
        value = text(between(block, b">", b"</common:external-id-normalized>", i)) if i >= 0 else ""
        value = (value or text(between(block, b"<common:external-id-value>", b"</common:external-id-value>"))).lower()
        if kind == b"doi":
            doi = DOI.search(value)
            if doi:
                found.add("d:" + doi.group(0))
        elif kind == b"pmid":
            digits = value.replace("pmid", "").strip(" :")
            if digits.isdigit():
                found.add("p:" + str(int(digits)))
        else:
            arxiv = ARXIV.search(value.strip())
            if arxiv:
                found.add("d:10.48550/arxiv." + arxiv.group(1))
    return found


def parse(chunk):
    """A piece of the unpacked archive holding whole records -> (output lines, records, records with a public
    name, records with works, identifiers). Between the records sit the headers of the tar format: ignored."""
    lines = []
    n = with_name = with_works = n_ids = 0
    for xml in chunk.split(RECORD)[1:]:
        orcid = (between(xml, b"<common:path>", b"</common:path>") or b"").decode("ascii", "replace").strip()
        if len(orcid) != 19:
            continue
        n += 1
        given = family = credit = ""
        i = xml.find(b'<person:name visibility="public"')
        if i >= 0:
            j = xml.find(b"</person:name>", i)
            given = text(between(xml, b"<personal-details:given-names>", b"</personal-details:given-names>", i, j))
            family = text(between(xml, b"<personal-details:family-name>", b"</personal-details:family-name>", i, j))
            credit = text(between(xml, b"<personal-details:credit-name>", b"</personal-details:credit-name>", i, j))
        other = []
        i = xml.find(b"<other-name:other-names ")
        j = xml.find(b"</other-name:other-names>", i) if i >= 0 else -1
        if j >= 0:
            for entry in xml[i:j].split(b"<other-name:other-name ")[1:]:
                if b'visibility="public"' in entry[:entry.find(b">")]:
                    other.append(text(between(entry, b"<other-name:content>", b"</other-name:content>")))
        other = "|".join(dict.fromkeys(filter(None, other)))
        i = xml.find(b"<activities:works ")
        j = xml.find(b"</activities:works>", i) if i >= 0 else -1
        ids = sorted(identifiers(xml[i:j])) if j >= 0 else []
        if not (given or family or credit or ids):
            continue
        with_name += bool(given or family)
        with_works += bool(ids)
        n_ids += len(ids)
        lines.append("\t".join((orcid, given, family, credit, other, " ".join(ids))))
    return "\n".join(lines) + ("\n" if lines else ""), n, with_name, with_works, n_ids


def chunks(stream, slots):
    """The unpacked archive in pieces that end where a record ends. A slot is taken for every piece handed out
    (and given back when its result has been written), so the reader never runs far ahead of the workers."""
    rest = b""
    while True:
        data = stream.read(CHUNK)
        if not data:
            break
        data = rest + data
        cut = data.rfind(RECORD)
        if cut <= 0:                # no second record in sight yet: read on
            rest = data
            continue
        slots.acquire()
        yield data[:cut]
        rest = data[cut:]
    if rest:
        slots.acquire()
        yield rest


if __name__ == "__main__":
    t0 = time.time()
    gunzip = subprocess.Popen(["pigz", "-dc", archive] if shutil.which("pigz") else ["zcat", archive], stdout=subprocess.PIPE, bufsize=CHUNK)
    totals = [0, 0, 0, 0]
    reported = 0
    slots = threading.BoundedSemaphore(WORKERS * 3)
    with gzip.open(output + ".tmp", "wt", encoding="utf-8", newline="\n", compresslevel=6) as out, mp.get_context("fork").Pool(WORKERS) as pool:
        out.write("orcid\tgiven\tfamily\tcredit\tother\tworks\n")
        for lines, *counts in pool.imap(parse, chunks(gunzip.stdout, slots), chunksize=1):
            slots.release()
            out.write(lines)
            totals = [a + b for a, b in zip(totals, counts)]
            if totals[0] - reported >= 1_000_000:
                reported = totals[0]
                print(f"  {totals[0]:,} records, {totals[1]:,} with a public name, {totals[2]:,} with works, {totals[3]:,} identifiers ({time.time() - t0:.0f}s)", flush=True)
    if gunzip.wait() != 0:
        raise SystemExit(f"reading {archive} failed")
    os.replace(output + ".tmp", output)
    print(f"Done. {totals[0]:,} records, {totals[1]:,} with a public name, {totals[2]:,} with works, {totals[3]:,} identifiers -> {output}; {time.time() - t0:.0f}s")
