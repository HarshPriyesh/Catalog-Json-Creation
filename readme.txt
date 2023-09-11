1. Create schema_file which will contain the table's schema information
2. Keep it at source_path
3. Update config.txt with source_path, file_name, and destination_path 
4. Execute run.py
5. Catalog_json will be created at destination_path

SET hive.execution.engine=tez;
SET hive.enforce.bucketing=true;
SET hive.tez.container.size=8192;
SET hive.tez.java.opts='-Xmx4000m';
SET hive.support.concurrency=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.exec.dynamic.partition.mode=nostrict;
