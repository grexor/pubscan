"""Extract the publications of authors with an ORCID from the OpenAlex snapshot.

Writes
  works.tab.gz               one row per work with at least one ORCID author (input of db_graph.py)
  works_topics.tab.gz        the OpenAlex topics and keywords of those works (input of db_topics.py)
  works_topics_vocab.tab.gz  the topic taxonomy seen in the snapshot: topic id, name, subfield, field, domain

A work is kept when at least one of its authors carries an ORCID and it either has a PubMed id or belongs to
the core of OpenAlex. The expansion pack (is_xpac) is left out: it consists mostly of data sets taken over from
repositories. Every kept work is written with its type; which types count as publications is decided later,
by db_graph.py, so that changing that choice does not need another pass over the snapshot.

Columns of works.tab.gz (tab separated, one line per work, no quoting; tabs and line breaks inside a field are
replaced by spaces):
  work_id    OpenAlex work id without the leading W
  pmid       PubMed id, or empty
  doi        DOI without https://doi.org/, or empty
  type       OpenAlex work type (article, review, preprint, book-chapter, dataset, ...)
  flags      p = paratext, r = retracted
  year       publication year, or empty
  date       publication date YYYY-MM-DD, or empty
  n_authors  number of authors of the work, with and without an ORCID
  title
  source     name of the journal, book series or repository
  orcids     ORCID iDs of its authors in the paper's author order, joined by ";"

Columns of works_topics.tab.gz: work_id, year, orcids, topics ("T10123:0.998;..."), keywords ("name:0.612;...").

The 2,000+ snapshot files are processed by WORKERS processes in parallel. Every worker writes the rows of one
snapshot file into parts_works/; the parts are concatenated at the end (a gzip file may consist of several
members). A run that was interrupted continues with the files that are not done yet; delete parts_works/ to
start from scratch. Lines are only JSON-parsed when they contain an ORCID. The snapshot is read with direct
I/O (dd), so that hours of reading do not push the data of a running server out of the page cache.
"""
import glob
import gzip
import json
import os
import shutil
import subprocess
import sys
import time
import multiprocessing as mp

input_files = sorted(glob.glob('openalex-snapshot/data/works/updated_date=*/*.gz'))  # only part files; works/ also holds deleted_ids.csv.gz
WORKS = 'works.tab.gz'
TOPICS = 'works_topics.tab.gz'
VOCAB = 'works_topics_vocab.tab.gz'
PARTS = 'parts_works'
WORKERS = int(os.environ.get('WORKERS', '6'))
ORCID_MARK = b'https://orcid.org/'
WORKS_HEADER = ['work_id', 'pmid', 'doi', 'type', 'flags', 'year', 'date', 'n_authors', 'title', 'source', 'orcids']
TOPICS_HEADER = ['work_id', 'year', 'orcids', 'topics', 'keywords']
BLANKS = str.maketrans({'\t': ' ', '\n': ' ', '\r': ' '})


def clean(text):
    """A field of a tab separated line: no tabs, no line breaks."""
    return str(text if text is not None else '').translate(BLANKS).strip()


def extract_pmid(work):
    ids = work.get('ids') or {}
    pmid_raw = ids.get('pmid')
    if pmid_raw:
        pmid_str = str(pmid_raw).replace('https://pubmed.ncbi.nlm.nih.gov/', '').strip()
        if pmid_str.isdigit():
            return pmid_str
    return ''


def short_id(url, prefix):
    """'https://openalex.org/T11464' -> 'T11464'"""
    return (url or '').replace(prefix, '')


def clean_term(text):
    """Keyword/topic names are stored ';'-separated with ':score', so neither character may stay inside a name."""
    return (text or '').replace(';', ' ').replace(':', ' ').replace('\t', ' ').replace('\n', ' ').replace('\r', ' ').strip()


def work_rows(work):
    """(works row, topics row, topics seen) of one work, or None when the work is not kept."""
    orcids = []
    for authorship in work.get('authorships') or []:
        orcid = (authorship.get('author') or {}).get('orcid')
        if orcid:
            orcids.append(orcid.replace('https://orcid.org/', '').strip())
    if not orcids:
        return None
    pmid = extract_pmid(work)
    if not pmid and work.get('is_xpac'):
        return None
    work_id = short_id(work.get('id'), 'https://openalex.org/W')
    if not work_id.isdigit():
        return None

    year = work.get('publication_year')
    year = str(year) if isinstance(year, int) else ''
    n_authors = work.get('authors_count')
    if not isinstance(n_authors, int):
        n_authors = len(work.get('authorships') or [])
    source = ((work.get('primary_location') or {}).get('source') or {}).get('display_name')
    flags = ('p' if work.get('is_paratext') else '') + ('r' if work.get('is_retracted') else '')
    orcid_list = ';'.join(orcids)
    row = [work_id, pmid, clean(short_id(work.get('doi'), 'https://doi.org/')), clean(work.get('type')), flags, year,
           clean(work.get('publication_date')), str(n_authors), clean(work.get('title')), clean(source), orcid_list]

    topics = []
    vocab = {}
    for t in work.get('topics') or []:
        tid = short_id(t.get('id'), 'https://openalex.org/')
        if not tid:
            continue
        topics.append(f"{tid}:{t.get('score') or 0:.3f}")
        vocab[tid] = (clean_term(t.get('display_name')),
                      clean_term((t.get('subfield') or {}).get('display_name')),
                      clean_term((t.get('field') or {}).get('display_name')),
                      clean_term((t.get('domain') or {}).get('display_name')))
    keywords = [f"{clean_term(k.get('display_name'))}:{k.get('score') or 0:.3f}"
                for k in work.get('keywords') or [] if clean_term(k.get('display_name'))]
    return row, [work_id, year, orcid_list, ';'.join(topics), ';'.join(keywords)], vocab


def process_file(index_path):
    """One snapshot file -> parts_works/<index>.works.gz, <index>.topics.gz and <index>.json (written last: its
    presence marks the file as done; it holds the row count and the topic vocabulary seen)."""
    index, filepath = index_path
    done = f'{PARTS}/{index:05d}.json'
    if os.path.exists(done):
        info = json.load(open(done))
        return index, filepath, info['rows'], info['vocab'], True
    vocab = {}
    n_rows = 0
    works_part = f'{PARTS}/{index:05d}.works.gz'
    topics_part = f'{PARTS}/{index:05d}.topics.gz'
    with gzip.open(works_part + '.tmp', 'wt', encoding='utf-8', newline='\n', compresslevel=6) as wout, \
         gzip.open(topics_part + '.tmp', 'wt', encoding='utf-8', newline='\n', compresslevel=6) as tout:
        dd = subprocess.Popen(['dd', 'if=' + filepath, 'iflag=direct', 'bs=4M', 'status=none'], stdout=subprocess.PIPE)
        zcat = subprocess.Popen(['zcat'], stdin=dd.stdout, stdout=subprocess.PIPE, bufsize=1 << 20)
        dd.stdout.close()
        for line in zcat.stdout:
            if ORCID_MARK not in line:
                continue
            rows = work_rows(json.loads(line))
            if rows is None:
                continue
            row, topics_row, seen = rows
            wout.write('\t'.join(row) + '\n')
            tout.write('\t'.join(topics_row) + '\n')
            vocab.update(seen)
            n_rows += 1
        if zcat.wait() != 0 or dd.wait() != 0:
            raise RuntimeError(f'reading {filepath} failed')
    os.replace(works_part + '.tmp', works_part)
    os.replace(topics_part + '.tmp', topics_part)
    with open(done + '.tmp', 'w') as f:
        json.dump({'file': filepath, 'rows': n_rows, 'vocab': vocab}, f)
    os.replace(done + '.tmp', done)
    return index, filepath, n_rows, vocab, False


def concatenate(output, header, suffix):
    """gzip members: header first, then every part in file order."""
    with open(output + '.tmp', 'wb') as out:
        out.write(gzip.compress(('\t'.join(header) + '\n').encode('utf-8')))
        for name in sorted(os.listdir(PARTS)):
            if name.endswith(suffix):
                with open(os.path.join(PARTS, name), 'rb') as part:
                    shutil.copyfileobj(part, out, 1 << 24)
    os.replace(output + '.tmp', output)


if __name__ == '__main__':
    t0 = time.time()
    os.makedirs(PARTS, exist_ok=True)
    for name in os.listdir(PARTS):          # leftovers of an interrupted run
        if name.endswith('.tmp'):
            os.remove(os.path.join(PARTS, name))
    vocab = {}
    total = 0
    print(f'{len(input_files)} files, {WORKERS} workers', flush=True)
    with mp.get_context('fork').Pool(WORKERS) as pool:
        for done, (index, filepath, n_rows, part_vocab, skipped) in enumerate(pool.imap_unordered(process_file, list(enumerate(input_files))), 1):
            vocab.update({tid: tuple(names) for tid, names in part_vocab.items()})
            total += n_rows
            if not skipped:
                print(f'[{done}/{len(input_files)}] {filepath}: {n_rows:,} rows ({total:,} total, {time.time() - t0:.0f}s)', flush=True)

    print('Concatenating parts ...', flush=True)
    concatenate(WORKS, WORKS_HEADER, '.works.gz')
    concatenate(TOPICS, TOPICS_HEADER, '.topics.gz')
    with gzip.open(VOCAB, 'wt', encoding='utf-8', newline='\n') as f:
        f.write('\t'.join(['topic_id', 'name', 'subfield', 'field', 'domain']) + '\n')
        for tid in sorted(vocab, key=lambda t: int(t[1:]) if t[1:].isdigit() else 0):
            f.write('\t'.join([tid, *vocab[tid]]) + '\n')
    shutil.rmtree(PARTS, ignore_errors=True)
    print(f'Done. {total:,} rows -> {WORKS}, {TOPICS}; {len(vocab):,} topics -> {VOCAB}; {time.time() - t0:.0f}s')
