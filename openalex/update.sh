#!/bin/bash
eval "$(micromamba shell hook --shell bash)"
micromamba activate pubscan

./1_download.sh
rm -f names.db
rm -f pubscan.db
python 2_authors.py
python 3_publications.py
python 4_combine.py
python db_names.py
python db_authors.py
python db_publications.py
