#!/bin/bash
eval "$(micromamba shell hook --shell bash)"
micromamba activate pubscan

./1_download.sh              # the OpenAlex snapshot
# The ORCID Public Data File is published once a year (October). Put its "summaries" archive into orcid/:
#   https://info.orcid.org/documentation/integration-guide/working-with-bulk-data  (about 50 GB)
rm -f names.db
rm -rf graph topics
python orcid_records.py orcid/ORCID_*_summaries.tar.gz orcid_records.tab.gz   # the names as the ORCID records state them
python 2_authors.py          # authors.tab.gz: the OpenAlex author records with an ORCID
python 3_publications.py     # works.tab.gz, works_topics.tab.gz: the works with an ORCID author
python db_graph.py works.tab.gz authors.tab.gz graph orcid_records.tab.gz          # co-authorship graph and title file (db/graph/)
python db_names.py graph names.db authors.tab.gz orcid_records.tab.gz              # name index of the search box (db/names.db)
python db_topics.py works_topics.tab.gz works_topics_vocab.tab.gz graph topics     # author-topic table for the topic search (db/topics/)
