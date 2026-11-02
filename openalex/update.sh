#!/bin/bash
eval "$(micromamba shell hook --shell bash)"
micromamba activate pubscan

./1_download.sh
rm -f names.db
rm -f pubscan.db
rm -rf graph topics
python 2_authors.py
python 3_publications.py
python 4_combine.py
python db_names.py
python db_authors.py
python db_publications.py
python db_graph.py pubscan.db graph   # memory-mapped graph for the server (db/graph/)
python db_topics.py topics.tab.gz topics_vocab.tab.gz graph topics   # author-topic table for the topic search (db/topics/)
