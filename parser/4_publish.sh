echo "drop table authors"
mysql pubscan -e "DROP TABLE IF EXISTS authors;"

echo "drop table publications"
mysql pubscan -e "DROP TABLE IF EXISTS publications;"

echo "rename table authors_temp to authors"
mysql pubscan -e "RENAME TABLE authors_temp TO authors;"

echo "rename table publications_temp to publications"
mysql pubscan -e "RENAME TABLE publications_temp TO publications;"
