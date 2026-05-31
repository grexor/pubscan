import gzip
import json
import glob
import csv

input_files = glob.glob('openalex-snapshot/data/works/**/*.gz', recursive=True)
output_file = 'publications.tab.gz'

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

with gzip.open(output_file, 'wt', newline='', encoding='utf-8') as tsv:
    writer = csv.writer(tsv, delimiter='\t')
    writer.writerow(['openalex_work_id', 'pmid', 'title', 'short_abstract', 'journal', 'year', 'author_orcids', 'author_orcid_names', 'all_author_names'])

    for filepath in input_files:
        print(f'Processing {filepath}...')
        with gzip.open(filepath, 'rt', encoding='utf-8') as f:
            for line in f:
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
                        orcid_names.append(name)

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

print(f'Done. Output saved to {output_file}')
