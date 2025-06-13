echo "split"
rm splits/publications_*
split -l 1000000 publications.tab splits/publications_

echo "remove table publications_temp"
mysql --local-infile=1 pubscan -e "DROP TABLE IF EXISTS publications_temp;"

echo "create table publications_temp"
mysql --local-infile=1 pubscan -e "CREATE TABLE publications_temp (pmid INTEGER, title TEXT NOT NULL, pub_year INTEGER, authors TEXT) ENGINE=InnoDB;"

for fname in `ls splits/publications_*`
do
    echo $fname
    mysql --local-infile=1 pubscan -e "SET autocommit=0; SET unique_checks=0; SET foreign_key_checks=0; SET sql_log_bin=0; LOAD DATA LOCAL INFILE \"${fname}\" INTO TABLE publications_temp FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'; COMMIT; SET unique_checks=1; SET foreign_key_checks=1; SET sql_log_bin=1;"
done

echo "create index on table publications_temp"
mysql pubscan -e "ALTER TABLE publications_temp ADD PRIMARY KEY (pmid), ADD INDEX (pmid);"
