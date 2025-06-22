#!/bin/bash

mkdir -p ../database

LOGFILE=$(mktemp)

# Run wget and log all output
wget -r -np -nH --cut-dirs=2 -A '*.gz' -P ../database -nc https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/ 2>&1 | tee "$LOGFILE"
wget -r -np -nH --cut-dirs=2 -A '*.gz' -P ../database -nc ftp://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/ 2>&1 | tee -a "$LOGFILE"

python 1_parse.py
./2_authors.sh
./3_publications.sh
./4_publish.sh
mysql pubscan -e "INSERT INTO info () VALUES ();"    
