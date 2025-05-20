
mkdir -p ../database
wget -r -np -nH --cut-dirs=2 -A '*.gz' -P ../database -nc https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/
wget -r -np -nH --cut-dirs=2 -A '*.gz' -P ../database -nc ftp://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/
