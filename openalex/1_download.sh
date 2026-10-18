#!/bin/bash
# Mirror the OpenAlex snapshot (quarterly releases; https://help.openalex.org/access/sync/).
# Since the 2026 layout the data lives under s3://openalex/data/jsonl/<entity>/updated_date=*/part_*.gz;
# the old s3://openalex/data/<entity>/ prefixes are empty. Local layout stays openalex-snapshot/data/<entity>/.
set -euo pipefail

BUCKET="s3://openalex/data/jsonl"
LOCAL="openalex-snapshot/data"

for entity in works authors institutions; do
    # Safety: a sync with --delete against an empty or moved remote prefix wipes the local copy.
    # Only proceed if the release manifest is present (OpenAlex writes it last, so present = release complete).
    if ! aws s3 ls "${BUCKET}/${entity}/manifest.json" --no-sign-request >/dev/null 2>&1; then
        echo "ERROR: ${BUCKET}/${entity}/manifest.json not found; layout changed or release in progress. Not syncing." >&2
        exit 1
    fi
    echo "Syncing ${entity} ..."
    aws s3 sync "${BUCKET}/${entity}" "${LOCAL}/${entity}" \
        --no-sign-request \
        --exact-timestamps \
        --delete
done

echo "Snapshot release dates:"
for entity in works authors institutions; do
    printf "  %-13s %s\n" "${entity}" "$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['date'])" "${LOCAL}/${entity}/manifest.json")"
done
