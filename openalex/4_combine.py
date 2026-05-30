import gzip
import csv
from collections import defaultdict

AUTHORS_FILE      = 'authors_with_orcid.tsv.gz'
PUBLICATIONS_FILE = 'publications_with_orcids.tsv.gz'
OUTPUT_FILE       = 'authors_final.tsv.gz'

# === Step 1: load orcid -> display_name from authors file ===
print("Loading authors...")
orcid_to_name = {}  # orcid_number -> display_name
with gzip.open(AUTHORS_FILE, 'rt', encoding='utf-8') as f:
    reader = csv.DictReader(f, delimiter='\t')
    for row in reader:
        orcid = row['orcid'].replace('https://orcid.org/', '').strip()
        name  = row['name'].strip()
        if orcid and name:
            orcid_to_name[orcid] = name
print(f"  {len(orcid_to_name):,} authors with ORCID loaded")

# === Step 2: scan publications, accumulate pmids per orcid ===
print("Scanning publications...")
orcid_to_pmids = defaultdict(set)
i = 0
skipped = 0
with gzip.open(PUBLICATIONS_FILE, 'rt', encoding='utf-8') as f:
    reader = csv.DictReader(f, delimiter='\t')
    for row in reader:
        pmid = row.get('pmid', '').strip()
        if not pmid:
            skipped += 1
            continue
        orcids = row.get('author_orcids', '').strip()
        if not orcids:
            continue
        for orcid in orcids.split(';'):
            orcid = orcid.strip()
            if orcid:
                orcid_to_pmids[orcid].add(pmid)
        i += 1
        if i % 500_000 == 0:
            print(f"  {i:,} publications processed...")
print(f"  {i:,} publications processed, {skipped:,} skipped (no PMID)")

# === Step 3: write output ===
print("Writing output...")
written = 0
with gzip.open(OUTPUT_FILE, 'wt', encoding='utf-8') as f:
    writer = csv.writer(f, delimiter='\t')
    writer.writerow(['author_name', 'pmids'])
    for orcid, name in orcid_to_name.items():
        pmids = orcid_to_pmids.get(orcid)
        if not pmids:
            continue  # skip authors with no publications in our dataset
        author_name = f"{name} {orcid}"
        pmids_str   = ','.join(sorted(pmids, key=lambda x: int(x) if x.isdigit() else 0))
        writer.writerow([author_name, pmids_str])
        written += 1
        if written % 500_000 == 0:
            print(f"  {written:,} authors written...")

print(f"Done. {written:,} authors written to {OUTPUT_FILE}")
