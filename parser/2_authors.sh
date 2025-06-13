echo "split"
rm splits/authors_*
split -l 1000000 authors.tab splits/authors_

echo "remove table"
mysql pubscan -e "DROP TABLE IF EXISTS authors_temp;"

echo "create table authors_temp"
mysql pubscan -e "CREATE TABLE authors_temp (author_name VARCHAR(255) NOT NULL, pmids TEXT) ENGINE=InnoDB;"

for fname in `ls splits/authors_*`
do
    echo $fname
    mysql pubscan -e "SET autocommit=0; SET unique_checks=0; SET foreign_key_checks=0; SET sql_log_bin=0; LOAD DATA LOCAL INFILE \"${fname}\" INTO TABLE authors_temp FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'; COMMIT; SET unique_checks=1; SET foreign_key_checks=1; SET sql_log_bin=1;"
done

echo "create index on table authors_temp"
mysql pubscan -e "ALTER TABLE authors_temp ADD PRIMARY KEY (author_name), ADD INDEX (author_name);"

# file containing only names, much faster to grep for author names
cut -f 1 authors.tab > authors_names.tab
