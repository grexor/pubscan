#!/bin/bash

mkdir -p ../database

LOGFILE=$(mktemp)

# Run wget and log all output
wget -r -np -nH --cut-dirs=2 -A '*.gz' -P ../database -nc https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/ 2>&1 | tee "$LOGFILE"
wget -r -np -nH --cut-dirs=2 -A '*.gz' -P ../database -nc ftp://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/ 2>&1 | tee -a "$LOGFILE"

# Look for 'saved [' with both straight and curly quotes
NEW_FILES=$(grep -E '[‘'\'']\.\./database/[^’'\'']+\.gz[’'\''] saved \[[0-9]+' "$LOGFILE")

if [ -n "$NEW_FILES" ]; then
    # if new files were downloaded, process and update the databases
    echo "New files downloaded:"
    echo "$NEW_FILES" | sed -E "s/.*[‘'\''](.*\.gz)[’'\''].*/\1/"
    python 1_parse.py
    python 2_db.py
else
    # otherwise do nothing
    echo "No new files were downloaded."
fi

rm "$LOGFILE"
