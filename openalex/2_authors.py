import gzip
import json
import glob
import csv

input_files = glob.glob('openalex-snapshot/data/authors/**/*.gz', recursive=True)
output_file = 'authors.tab.gz'

with gzip.open(output_file, 'wt', newline='', encoding='utf-8') as tsv:
    writer = csv.writer(tsv, delimiter='\t')
    writer.writerow(['openalex_id', 'name', 'orcid'])

    for filepath in input_files:
        print(f'Processing {filepath}...')
        with gzip.open(filepath, 'rt', encoding='utf-8') as f:
            for line in f:
                author = json.loads(line)
                orcid = author.get('orcid')
                if orcid:  # skip authors without ORCID
                    writer.writerow([
                        author.get('id'),
                        author.get('display_name'),
                        orcid
                    ])

print(f'Done. Output saved to {output_file}')
