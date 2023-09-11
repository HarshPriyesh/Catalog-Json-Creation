1. Create schema_file which will contain the table's schema information
2. Keep it at source_path
3. Update config.txt with source_path, file_name, and destination_path 
4. Execute run.py
5. Catalog_json will be created at destination_path

STORED AS ORC
LOCATION 'gs://ap-edhsta-bld-01-stb-euwe2-coblstaging-01/XMLSOURCEPATH/LBG/';
