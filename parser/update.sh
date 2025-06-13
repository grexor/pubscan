./0_download.sh
python 1_parse.py
./2_authors.sh
./3_publications.sh
./4_publish.sh

mysql pubscan -e "INSERT INTO info () VALUES ();"