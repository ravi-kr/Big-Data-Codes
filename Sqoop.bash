Apache SQOOP:

SQOOP Import:

# To import table data to hdfs default path

sqoop import \
-\-connect jdbc:mysql://localhost/retail_db \
-\-username root \
-\-password cloudera \
-\-table orders \
-\-fields-terminated-by ',';

# To mysql table data to hdfs specific path

sqoop import \
-\-connect jdbc:mysql://localhost/retail_db \
-\-username root \
-\-password cloudera \
-\-table orders \
-\-fields-terminated-by ',' \
-\-target-dir 'user/data/orders';

# To import mysql table specific (filtered) data to hdfs

sqoop import \
-\-connect jdbc:mysql://localhost/retail_db \
-\-username root \
-\-password cloudera \
-\-table orders \
-\-where "order_id<10" \
-\-fields-terminated-by ',' \
-\-target-dir 'user/data/orders';

sqoop import \
-\-connect jdbc:mysql://localhost/retail_db \
-\-username root \
-\-password cloudera \
-\-table orders \
-\-where "order_id<10" \
-\-fields-terminated-by ',' \
-\-target-dir 'user/data/orders'\
-m 1;

# To import mysql table specific columns data to hdfs

sqoop import \
-\-connect jdbc:mysql://localhost/retail_db \
-\-username root \
-\-password cloudera \
-\-table orders \
-\-columns "order_id, order_date" \
-\-fields-terminated-by ',' \
-\-target-dir 'user/data/orders6';

# Overwrite existing data while sqoop import

sqoop import \
-\-connect jdbc:mysql://localhost/retail_db \
-\-username root \
-\-password cloudera \
-\-table orders \
-\-fields-terminated-by ',' \
-\-target-dir 'user/data/orders6' \
-\-delete-target-dir;

# Controlling parallelism in Sqoop

sqoop import \
-\-connect jdbc:mysql://localhost/retail_db \
-\-username root \
-\-password cloudera \
-\-table orders \
-\-fields-terminated-by ',' \
-\-target-dir 'user/data/orders6' \
-\-delete-target-dir
-\-num-mappers 2;
/* -m 1; */

# Data Append while sqoop import

sqoop import \
-\-connect jdbc:mysql://localhost/retail_db \
-\-username root \
-\-password cloudera \
-\-table orders \
-\-fields-terminated-by ',' \
-\-target-dir 'user/data/orders6' \
-\-append;

# Import mysql table (No primary Key) data to hdfs

sqoop import \
-\-connect jdbc:mysql://localhost/retail_db \
-\-username root \
-\-password cloudera \
-\-table orders_nopk \
-\-fields-terminated-by ',' \
-\-target-dir 'user/data/orders6/' \
-\-delete-target-dir;
-\--\--\--\--\--\--\--\--\--\--\--\--\--\--\--\--\--\--\--\--\--\--\--
sqoop import \
-\-connect jdbc:mysql://localhost/retail_db \
-\-username root \
-\-password cloudera \
-\-table orders_nopk \
-\-fields-terminated-by ',' \
-\-target-dir 'user/data/orders6/' \
-\-delete-target-dir
-\-split-by order_id;
(or)
sqoop import \
-\-connect jdbc:mysql://localhost/retail_db \
-\-username root \
-\-password cloudera \
-\-table orders_nopk \
-\-fields-terminated-by ',' \
-\-target-dir 'user/data/orders6/' \
-\-delete-target-dir
-m 1;

# Storing Sqoop output data in textfile format
# Delimited text is the default import format

sqoop import \
-\-connect jdbc:mysql://localhost/retail_db \
-\-username root \
-\-password cloudera \
-\-table orders \
-\-target-dir 'user/data/orders_text/' \
-\-as-textfile \
-\-delete-target-dir;

# Storing Sqoop output data in sequence file format
# SequenceFiles are a binary format that stores individual records in custom record-specific data types

sqoop import \
-\-connect jdbc:mysql://localhost/retail_db \
-\-username root \
-\-password cloudera \
-\-table orders \
-\-target-dir 'user/data/orders_sequence/' \
-\-as-sequencefile \
-\-delete-target-dir;

# Storing Sqoop output data in avro file format

sqoop import \
-\-connect jdbc:mysql://localhost/retail_db \
-\-username root \
-\-password cloudera \
-\-table orders \
-\-target-dir 'user/data/orders_avro/' \
-\-as-avrodatafile \
-\-delete-target-dir;

# If the above job fail due to any external lib/jar file then please include the below statement and then run

sqoop import /* -Dmapreduce.job.user.classpath.first=true */ \

# Storing Sqoop output data in parquet file format

sqoop import \
-\-connect jdbc:mysql://localhost/retail_db \
-\-username root \
-\-password cloudera \
-\-table orders \
-\-target-dir 'user/data/orders_parquet/' \
-\-as-parquetfile \
-\-delete-target-dir;

# Output data compression in Sqoop

sqoop import \
-\-connect jdbc:mysql://localhost/retail_db \
-\-username root \
-\-password cloudera \
-\-table orders \
-\-target-dir 'user/data/orders_compress/' \
-\-compress \
-\-delete-target-dir;

# Running custom SQL query with sqoop

sqoop import \
-\-connect jdbc:mysql://localhost/retail_db \
-\-username root \
-\-password cloudera \
-\-table orders \
-\-target-dir 'user/data/orders/' \
-\-query "select * from orders where order_status = 'PROCESSING' and \$CONDITIONS " \
-\-split-by order_id \
-\-delete-target-dir;

# Importing all tables from a relational db to hadoop

sqoop import-all-tables \
-\-connect jdbc:mysql://localhost/retail_db \
-\-username root \
-\-password cloudera \
-\-warehouse-dir 'user/data/sqoop_all_tables/' \
-\-exclude-tables "orders,customers,orders_delta" \
-\-autorest-to-one-mapper;


#Note:
#ALL columns must be imported
#should use autorest-to-one-mapper if there are any no pk tables
#split by can't be used


# Import data from a relation DB table to Hive table by Sqoop

sqoop import \
-\-connect jdbc:mysql://localhost/retail_db \
-\-username root \
-\-password cloudera \
-\-table orders_delta \
-\-hive-import \
-\-hive-database hive_sqoop \
-\-hive-table hive_sq_table \
-m 1;

# Sqoop incremental loads (lastmodified, append)

sqoop import \
-\-connect jdbc:mysql://localhost/retail_db \
-\-username root \
-\-password cloudera \
-\-table orders_delta \
-\-target-dir 'user/data/orders_delta/' \
-\-as-textfile \
-\-delete-target-dir \
-m 1;
-\--\--\--\--\--\--\--\--\--\--\--\--\--\--\--\--\--\--\--\--\--\--\--
sqoop import \
-\-connect jdbc:mysql://localhost/retail_db \
-\-username root \
-\-password cloudera \
-\-table orders_delta \
-\-target-dir 'user/data/orders_delta/' \
-\-check-column order_date \
-\-incremental append \
-\-last-value "2022-05-29 01:07:39" \
-m 1;

# Sqoop option file

Create a new option file by using the below commands

import
-\-connect
jdbc:mysql://localhost/retail_db
-\-username
root
-\-password
cloudera

Use the below sample command to use sqoop options file

sqoop -\-options-file /home/cloudera/sqoop_connect -\-table orders -\-target-dir '/user/data/orders_today'

# Sqoop Export

# Export hdfs data to mysql table
# *table in mysql must be present

sqoop export \
-\-connect jdbc:mysql://localhost/sqoop_export \
-\-username root \
-\-password cloudera \
-\-export-dir '/user/data/orders_new/' \
-\-table orders_sqoop \
-\-input-fields-terminated-by ',';

# Sqoop exports sepcific columns

sqoop export \
-\-connect jdbc:mysql://localhost/sqoop_export \
-\-username root \
-\-password cloudera \
-\-export-dir '/user/data/orders_new/' \
-\-table orders_sqoop_dt \
-\-columns "order_id, order_date, customer_id, order_status" \
-\-input-fields-terminated-by ',';

# Sqoop Delta load exports

**************
-\-update-key
-\-update-mode (allowinsert,updateonly)
**************

sqoop export \
-\-connect jdbc:mysql://localhost/sqoop_export \
-\-username root \
-\-password cloudera \
-\-export-dir '/user/data/orders_new/' \
-\-update-key order_id \
-\-update-mode allowinsert \
-\-table orders_sqoop_dt \
-\-columns "order_id, order_date, customer_id, order_status" \
-\-input-fields-terminated-by ',';
