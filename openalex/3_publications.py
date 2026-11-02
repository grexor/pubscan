"""Extract PubMed works from the OpenAlex snapshot.

Writes
  publications.tab.gz  one row per work with a PMID and at least one ORCID author (input of db_publications.py)
  topics.tab.gz        the OpenAlex topics and keywords of those works (input of db_topics.py)
  topics_vocab.tab.gz  the topic taxonomy seen in the snapshot: topic id, name, subfield, field, domain

The 2,000+ snapshot files are processed by WORKERS processes in parallel; every worker writes its own
gzip members into parts/, which are concatenated at the end (a gzip file may consist of several members).
Lines are only JSON-parsed when they contain a PubMed id, which skips most of the snapshot cheaply.
"""
import glob
import gzip
import json
import csv
import os
import shutil
import subprocess
import sys
import time
import multiprocessing as mp

input_files = sorted(glob.glob('openalex-snapshot/data/works/updated_date=*/*.gz'))  # only part files; works/ also holds deleted_ids.csv.gz
PUBLICATIONS = 'publications.tab.gz'
TOPICS = 'topics.tab.gz'
VOCAB = 'topics_vocab.tab.gz'
PARTS = 'parts'
WORKERS = int(os.environ.get('WORKERS', '6'))
PMID_MARK = b'pubmed.ncbi.nlm.nih.gov'

# load author orcid -> name mapping from the authors table
author_lookup = {}
with gzip.open('authors.tab.gz', 'rt', encoding='utf-8') as f:
    reader = csv.DictReader(f, delimiter='\t')
    for row in reader:
        orcid = row['orcid'].replace('https://orcid.org/', '')
        author_lookup[orcid] = row['name']


def reconstruct_abstract(inverted_index):
    if not inverted_index:
        return ''
    positions = []
    for word, pos_list in inverted_index.items():
        for pos in pos_list:
            positions.append((pos, word))
    positions.sort()
    abstract = ' '.join(word for _, word in positions)
    if len(abstract) > 300:
        abstract = abstract[:300].rsplit(' ', 1)[0] + '...'
    return abstract


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
    return (text or '').replace(';', ' ').replace(':', ' ').replace('\t', ' ').strip()


def process_file(index_path):
    """One snapshot file -> parts/<index>.pub.gz, parts/<index>.topics.gz, and the topic vocabulary seen."""
    index, filepath = index_path
    vocab = {}
    n_rows = 0
    with gzip.open(f'{PARTS}/{index:05d}.pub.gz', 'wt', newline='', encoding='utf-8') as tsv, \
         gzip.open(f'{PARTS}/{index:05d}.topics.gz', 'wt', newline='', encoding='utf-8') as ttsv:
        writer = csv.writer(tsv, delimiter='\t')
        twriter = csv.writer(ttsv, delimiter='\t')
        proc = subprocess.Popen(['zcat', filepath], stdout=subprocess.PIPE, bufsize=1 << 20)
        for line in proc.stdout:
            if PMID_MARK not in line:
                continue
            work = json.loads(line)

            # skip early if no PMID — avoids parsing authors unnecessarily
            pmid = extract_pmid(work)
            if not pmid:
                continue

            orcids = []
            orcid_names = []
            all_names = []

            for authorship in work.get('authorships', []):
                author = authorship.get('author', {})
                display_name = author.get('display_name', '')
                orcid = author.get('orcid')
                all_names.append(display_name)
                if orcid:
                    orcid_number = orcid.replace('https://orcid.org/', '')
                    orcids.append(orcid_number)
                    name = author_lookup.get(orcid_number) or display_name
                    # the list is stored joined by ";" and later split on ","; a name containing either
                    # character would shift the names of every following author
                    orcid_names.append(name.replace(";", " ").replace(",", " "))

            if not orcids:
                continue

            location = work.get('primary_location') or {}
            source = location.get('source') or {}
            journal = source.get('display_name', '')

            abstract = reconstruct_abstract(work.get('abstract_inverted_index'))

            writer.writerow([
                work.get('id'),
                pmid,
                work.get('title'),
                abstract,
                journal,
                work.get('publication_year'),
                ';'.join(orcids),
                ';'.join(orcid_names),
                ';'.join(all_names),
            ])

            topics = []
            for t in work.get('topics') or []:
                tid = short_id(t.get('id'), 'https://openalex.org/')
                if not tid:
                    continue
                topics.append(f"{tid}:{t.get('score') or 0:.3f}")
                if tid not in vocab:
                    vocab[tid] = (clean_term(t.get('display_name')),
                                  clean_term((t.get('subfield') or {}).get('display_name')),
                                  clean_term((t.get('field') or {}).get('display_name')),
                                  clean_term((t.get('domain') or {}).get('display_name')))
            keywords = [f"{clean_term(k.get('display_name'))}:{k.get('score') or 0:.3f}"
                        for k in work.get('keywords') or [] if clean_term(k.get('display_name'))]
            twriter.writerow([pmid, work.get('publication_year'), ';'.join(orcids), ';'.join(topics), ';'.join(keywords)])
            n_rows += 1
        if proc.wait() != 0:
            raise RuntimeError(f'zcat failed on {filepath}')
    return index, filepath, n_rows, vocab


def concatenate(output, header, suffix):
    """gzip members: header first, then every part in file order."""
    with open(output + '.tmp', 'wb') as out:
        out.write(gzip.compress(('\t'.join(header) + '\r\n').encode('utf-8')))  # csv's line terminator
        for name in sorted(os.listdir(PARTS)):
            if name.endswith(suffix):
                with open(os.path.join(PARTS, name), 'rb') as part:
                    shutil.copyfileobj(part, out, 1 << 24)
    os.replace(output + '.tmp', output)


if __name__ == '__main__':
    t0 = time.time()
    shutil.rmtree(PARTS, ignore_errors=True)
    os.makedirs(PARTS)
    vocab = {}
    total = 0
    print(f'{len(input_files)} files, {WORKERS} workers', flush=True)
    with mp.get_context('fork').Pool(WORKERS) as pool:
        for done, (index, filepath, n_rows, part_vocab) in enumerate(pool.imap_unordered(process_file, list(enumerate(input_files))), 1):
            vocab.update(part_vocab)
            total += n_rows
            print(f'[{done}/{len(input_files)}] {filepath}: {n_rows:,} rows ({total:,} total, {time.time() - t0:.0f}s)', flush=True)

    print('Concatenating parts ...', flush=True)
    concatenate(PUBLICATIONS, ['openalex_work_id', 'pmid', 'title', 'short_abstract', 'journal', 'year', 'author_orcids', 'author_orcid_names', 'all_author_names'], '.pub.gz')
    concatenate(TOPICS, ['pmid', 'year', 'author_orcids', 'topics', 'keywords'], '.topics.gz')
    with gzip.open(VOCAB, 'wt', newline='', encoding='utf-8') as f:
        w = csv.writer(f, delimiter='\t')
        w.writerow(['topic_id', 'name', 'subfield', 'field', 'domain'])
        for tid in sorted(vocab, key=lambda t: int(t[1:]) if t[1:].isdigit() else 0):
            w.writerow([tid, *vocab[tid]])
    shutil.rmtree(PARTS, ignore_errors=True)
    print(f'Done. {total:,} rows -> {PUBLICATIONS}, {TOPICS}; {len(vocab):,} topics -> {VOCAB}; {time.time() - t0:.0f}s')
