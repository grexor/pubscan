echo "drop table authors"
sudo mysql pubscan -e "DROP TABLE IF EXISTS authors;"

echo "drop table publications"
sudo mysql pubscan -e "DROP TABLE IF EXISTS publications;"

echo "rename table authors_temp to authors"
sudo mysql pubscan -e "RENAME TABLE authors_temp TO authors;"

echo "rename table publications_temp to publications"
sudo mysql pubscan -e "RENAME TABLE publications_temp TO publications;"
