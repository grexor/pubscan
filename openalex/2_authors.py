"""Extract the authors with an ORCID from the OpenAlex snapshot.

usage: python 2_authors.py [output file]      (default: authors.tab.gz)

Writes authors.tab.gz (tab separated, csv quoting), one row per OpenAlex author record with an ORCID:
  openalex_id    https://openalex.org/A...
  name           the display name of the record (db_graph.py shows it when the ORCID record offers no better name)
  orcid          https://orcid.org/...
  alternatives   the other spellings OpenAlex knows for the author (full name, name alternatives), joined by "|";
                 db_names.py makes them searchable, so that an author is found under any of them
  works_count    number of works of the record
  affiliation    the main affiliation of the record: of the institutions OpenAlex has seen on the author's works,
                 the one of the newest year (among several of that year the one with the most years); empty when
                 OpenAlex knows none. The page shows it with the publications of an author (db_affiliations.py)
  affiliation_country   its country, as a two-letter code
  affiliation_year      the newest year in which a work of the author named the institution
  affiliation_id        its OpenAlex id (I...)

Several records can carry the same ORCID (db_graph.py then takes the name of the one with the most works). The
snapshot files are processed by WORKERS processes in parallel and read with direct I/O (dd), so that reading
them does not push the data of a running server out of the page cache; the rows are written in the order of
the sorted file names.
"""
import csv
import glob
import gzip
import io
import json
import os
import shutil
import subprocess
import sys
import time
import multiprocessing as mp

input_files = sorted(glob.glob('openalex-snapshot/data/authors/updated_date=*/*.gz'))  # only part files; the folder also holds manifest.json
OUTPUT = sys.argv[1] if len(sys.argv) > 1 else 'authors.tab.gz'
PARTS = 'parts_authors'
WORKERS = int(os.environ.get('WORKERS', '6'))
ORCID_MARK = b'orcid.org/'
MAX_ALTERNATIVES = 30
BLANKS = str.maketrans({'\t': ' ', '\n': ' ', '\r': ' ', '|': ' '})


def clean(text):
    return ' '.join(str(text or '').translate(BLANKS).split())


def main_affiliation(author):
    """(name, country code, newest year, OpenAlex id) of the institution the author is at now, as far as the works
    tell: the affiliation with the newest year, among several of that year the one with the most years (the first
    listed among equals). Without dated affiliations the first "last known" institution, with no year."""
    best = None
    for affiliation in author.get('affiliations') or []:
        institution = affiliation.get('institution') or {}
        name = clean(institution.get('display_name'))
        years = [y for y in affiliation.get('years') or [] if isinstance(y, int)]
        if not name or not years:
            continue
        key = (max(years), len(years))
        if best is None or key > best[0]:
            best = (key, name, institution)
    if best is None:
        for institution in author.get('last_known_institutions') or []:
            name = clean((institution or {}).get('display_name'))
            if name:
                best = ((0, 0), name, institution)
                break
    if best is None:
        return '', '', '', ''
    (year, _), name, institution = best
    return (name, clean(institution.get('country_code')).upper()[:2], year or '',
            str(institution.get('id') or '').rsplit('/', 1)[-1])


def process_file(index_path):
    """One snapshot file -> parts_authors/<index>.gz; returns (index, file, rows)."""
    index, filepath = index_path
    part = f'{PARTS}/{index:05d}.gz'
    n_rows = 0
    with gzip.open(part + '.tmp', 'wt', encoding='utf-8', newline='', compresslevel=6) as out:
        writer = csv.writer(out, delimiter='\t')
        dd = subprocess.Popen(['dd', 'if=' + filepath, 'iflag=direct', 'bs=4M', 'status=none'], stdout=subprocess.PIPE)
        zcat = subprocess.Popen(['zcat'], stdin=dd.stdout, stdout=subprocess.PIPE, bufsize=1 << 20)
        dd.stdout.close()
        for line in zcat.stdout:
            if ORCID_MARK not in line:
                continue
            author = json.loads(line)
            orcid = author.get('orcid')
            if not orcid:                       # the mark was somewhere else in the record
                continue
            name = author.get('display_name')
            seen = {clean(name)}
            alternatives = []
            for other in [author.get('full_name')] + list(author.get('display_name_alternatives') or []):
                other = clean(other)
                if other and other not in seen:
                    seen.add(other)
                    alternatives.append(other)
            works_count = author.get('works_count')
            writer.writerow([author.get('id'), name, orcid, '|'.join(alternatives[:MAX_ALTERNATIVES]),
                             works_count if isinstance(works_count, int) else 0, *main_affiliation(author)])
            n_rows += 1
        if zcat.wait() != 0 or dd.wait() != 0:
            raise RuntimeError(f'reading {filepath} failed')
    os.replace(part + '.tmp', part)
    return index, filepath, n_rows


if __name__ == '__main__':
    t0 = time.time()
    shutil.rmtree(PARTS, ignore_errors=True)
    os.makedirs(PARTS)
    total = 0
    print(f'{len(input_files)} files, {WORKERS} workers', flush=True)
    with mp.get_context('fork').Pool(WORKERS) as pool:
        for done, (index, filepath, n_rows) in enumerate(pool.imap_unordered(process_file, list(enumerate(input_files))), 1):
            total += n_rows
            print(f'[{done}/{len(input_files)}] {filepath}: {n_rows:,} rows ({total:,} total, {time.time() - t0:.0f}s)', flush=True)
    with open(OUTPUT + '.tmp', 'wb') as out:    # gzip members: the header, then every part in file order
        header = io.StringIO()
        csv.writer(header, delimiter='\t').writerow(['openalex_id', 'name', 'orcid', 'alternatives', 'works_count',
                                                          'affiliation', 'affiliation_country', 'affiliation_year', 'affiliation_id'])
        out.write(gzip.compress(header.getvalue().encode('utf-8')))
        for name in sorted(os.listdir(PARTS)):
            with open(os.path.join(PARTS, name), 'rb') as part:
                shutil.copyfileobj(part, out, 1 << 24)
    os.replace(OUTPUT + '.tmp', OUTPUT)
    shutil.rmtree(PARTS, ignore_errors=True)
    print(f'Done. {total:,} author records with an ORCID -> {OUTPUT}; {time.time() - t0:.0f}s')
