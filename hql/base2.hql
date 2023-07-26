#RAW.
create database if not exists RAW_BASE2_AL00242;
DROP TABLE IF EXISTS RAW_BASE2_AL00242.arbitrations;
CREATE EXTERNAL TABLE RAW_BASE2_AL00242.arbitrations(
`record_id` string,
`reasoncode` string,
`amount` string,
`pan` string,
`transaction_entry_value_date` string,
`merchant` string,
`infopac_report_criteria` string,
`note` string,
`debit_credit_indicator` string,
`heritage` string,
`gti` string)
PARTITIONED BY (
`ingestion_year` int,
`ingestion_month` int,
`ingestion_day` int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 'gs://${hivevar:rawBucket}/BASE2_AL00242/BASEII_ARBITRATIONS/';



create database if not exists RAW_BASE2_AL00242;
DROP TABLE IF EXISTS RAW_BASE2_AL00242.chargebacks;
CREATE EXTERNAL TABLE RAW_BASE2_AL00242.chargebacks(
`record_id` string,
`pan` string,
`amount` string,
`purchdate` string,
`reimbatt` string,
`arn` string,
`vrol_financial_id` string,
`merchant` string,
`city` string,
`trancode` string,
`entryvaluedate` string,
`heritagecode` string,
`debit_credit_indicator` string,
`heritage` string,
`gti` string)
PARTITIONED BY (
`ingestion_year` int,
`ingestion_month` int,
`ingestion_day` int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 'gs://${hivevar:rawBucket}/BASE2_AL00242/BASEII_CHARGEBACKS/';



create database if not exists RAW_BASE2_AL00242;
DROP TABLE IF EXISTS RAW_BASE2_AL00242.fraud_control;
CREATE EXTERNAL TABLE RAW_BASE2_AL00242.fraud_control(
`record_id` string,
`pan` string,
`amount` string,
`trantype` string,
`arn` string,
`reasoncode` string,
`authcode` string,
`merchant` string,
`city` string,
`termtrandate` string,
`postingdate` string,
`heritagecode` string,
`debit_credit_indicator` string,
`heritage` string,
`gti` string)
PARTITIONED BY (
`ingestion_year` int,
`ingestion_month` int,
`ingestion_day` int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 'gs://${hivevar:rawBucket}/BASE2_AL00242/BASEII_FRAUD_CONTROL/';


create database if not exists RAW_BASE2_AL00242;
DROP TABLE IF EXISTS RAW_BASE2_AL00242.representments;
CREATE EXTERNAL TABLE RAW_BASE2_AL00242.representments(
`record_id` string,
`pan` string,
`amount` string,
`trantype` string,
`arn` string,
`reasoncode` string,
`authcode` string,
`merchant` string,
`city` string,
`termtrandate` string,
`postingdate` string,
`heritagecode` string,
`debit_credit_indicator` string,
`heritage` string,
`gti` string,
`card_status` string)
PARTITIONED BY (
`ingestion_year` int,
`ingestion_month` int,
`ingestion_day` int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
LOCATION 'gs://${hivevar:rawBucket}/BASE2_AL00242/BASEII_REPRESENTMENTS/';

#VIEW
DROP VIEW IF EXISTS RAW_BASE2_AL00242.arbitrations_vw;
CREATE VIEW IF NOT EXISTS RAW_BASE2_AL00242.arbitrations_vw
AS SELECT
CAST (`arbitrations`.`record_id` AS VARCHAR(9)) AS `RECORD_ID`,
CAST (`arbitrations`.`reasoncode` AS INT) AS `REASONCODE`,
CAST (`arbitrations`.`amount` AS DECIMAL(15,2)) AS `AMOUNT`,
CAST (`arbitrations`.`pan` AS BIGINT) AS `PAN`,
CAST (`arbitrations`.`transaction_entry_value_date` AS STRING) AS `TRANSACTION_ENTRY_VALUE_DATE`,
CAST (`arbitrations`.`merchant` AS VARCHAR(25)) AS `MERCHANT`,
CAST (`arbitrations`.`infopac_report_criteria` AS VARCHAR(7)) AS `INFOPAC_REPORT_CRITERIA`,
CAST (`arbitrations`.`note` AS VARCHAR(80)) AS `NOTE`,
CAST (`arbitrations`.`debit_credit_indicator` AS VARCHAR(1)) AS `DEBIT_CREDIT_INDICATOR`,
CAST (`arbitrations`.`heritage` AS VARCHAR(3)) AS `HERITAGE`,
CAST (regexp_replace(`arbitrations`.`gti`,' ','') AS BIGINT) AS `GTI`,
`arbitrations`.`ingestion_year`,`arbitrations`.`ingestion_month`,`arbitrations`.`ingestion_day`
FROM RAW_BASE2_AL00242.arbitrations;

DROP VIEW IF EXISTS RAW_BASE2_AL00242.chargebacks_vw;
CREATE VIEW IF NOT EXISTS RAW_BASE2_AL00242.chargebacks_vw
AS SELECT
CAST (`chargebacks`.`record_id` AS VARCHAR(1)) AS `RECORD_ID`,
CAST (`chargebacks`.`pan` AS BIGINT) AS `PAN`,
CAST (`chargebacks`.`amount` AS DECIMAL(15,2)) AS `AMOUNT`,
CAST (`chargebacks`.`purchdate` AS STRING) AS `PURCHDATE`,
CAST (`chargebacks`.`reimbatt` AS INT) AS `REIMBATT`,
CAST (`chargebacks`.`arn` AS DECIMAL(23,0)) AS `ARN`,
CAST (`chargebacks`.`vrol_financial_id` AS VARCHAR(12)) AS `VROL_FINANCIAL_ID`,
CAST (`chargebacks`.`merchant` AS VARCHAR(25)) AS `MERCHANT`,
CAST (`chargebacks`.`city` AS VARCHAR(13)) AS `CITY`,
CAST (`chargebacks`.`trancode` AS INT) AS `TRANCODE`,
CAST (`chargebacks`.`entryvaluedate` AS STRING) AS `ENTRYVALUEDATE`,
CAST (`chargebacks`.`heritagecode` AS VARCHAR(4)) AS `HERITAGECODE`,
CAST (`chargebacks`.`debit_credit_indicator` AS VARCHAR(1)) AS `DEBIT_CREDIT_INDICATOR`,
CAST (`chargebacks`.`heritage` AS VARCHAR(3)) AS `HERITAGE`,
CAST (regexp_replace(`chargebacks`.`gti`,' ','') AS BIGINT) AS `GTI`,    
`chargebacks`.`ingestion_year`,`chargebacks`.`ingestion_month`,`chargebacks`.`ingestion_day`
FROM RAW_BASE2_AL00242.chargebacks;


DROP VIEW IF EXISTS RAW_BASE2_AL00242.fraud_control_vw;
CREATE VIEW IF NOT EXISTS RAW_BASE2_AL00242.fraud_control_vw
AS SELECT
CAST (`fraud_control`.`record_id` AS VARCHAR(1)) AS `RECORD_ID`,
CAST (`fraud_control`.`pan` AS BIGINT) AS `PAN`,
CAST (`fraud_control`.`amount` AS DECIMAL(15,2)) AS `AMOUNT`,
CAST (`fraud_control`.`trantype` AS VARCHAR(2)) AS `TRANTYPE`,
CAST (`fraud_control`.`arn` AS DECIMAL(23,0)) AS `ARN`,
CAST (`fraud_control`.`reasoncode` AS INT) AS `REASONCODE`,
CAST (`fraud_control`.`authcode` AS VARCHAR(6)) AS `AUTHCODE`,
CAST (`fraud_control`.`merchant` AS VARCHAR(25)) AS `MERCHANT`,
CAST (`fraud_control`.`city` AS VARCHAR(13)) AS `CITY`,
CAST (`fraud_control`.`termtrandate` AS STRING) AS `TERMTRANDATE`,
CAST (`fraud_control`.`postingdate` AS STRING) AS `POSTINGDATE`,
CAST (`fraud_control`.`heritagecode` AS VARCHAR(4)) AS `HERITAGECODE`,
CAST (`fraud_control`.`debit_credit_indicator` AS VARCHAR(1)) AS `DEBIT_CREDIT_INDICATOR`,
CAST (`fraud_control`.`heritage` AS VARCHAR(3)) AS `HERITAGE`,
CAST (regexp_replace(`fraud_control`.`gti`,' ','') AS BIGINT) AS `GTI`,                            
`fraud_control`.`ingestion_year`,`fraud_control`.`ingestion_month`,`fraud_control`.`ingestion_day`  
FROM RAW_BASE2_AL00242.fraud_control;


DROP VIEW IF EXISTS RAW_BASE2_AL00242.representments_vw;
CREATE VIEW IF NOT EXISTS RAW_BASE2_AL00242.representments_vw
AS SELECT
CAST (`representments`.`record_id` AS VARCHAR(1)) AS `RECORD_ID`,
CAST (`representments`.`pan` AS BIGINT) AS `PAN`,
CAST (`representments`.`amount` AS DECIMAL(15,2)) AS `AMOUNT`,
CAST (`representments`.`trantype` AS VARCHAR(2)) AS `TRANTYPE`,
CAST (`representments`.`arn` AS DECIMAL(23,0)) AS `ARN`,
CAST (`representments`.`reasoncode` AS INT) AS `REASONCODE`,
CAST (`representments`.`authcode` AS VARCHAR(6)) AS `AUTHCODE`,
CAST (`representments`.`merchant` AS VARCHAR(25)) AS `MERCHANT`,
CAST (`representments`.`city` AS VARCHAR(13)) AS `CITY`,
CAST (`representments`.`termtrandate` AS STRING) AS `TERMTRANDATE`,
CAST (`representments`.`postingdate` AS STRING) AS `POSTINGDATE`,
CAST (`representments`.`heritagecode` AS VARCHAR(4)) AS `HERITAGECODE`,
CAST (`representments`.`debit_credit_indicator` AS VARCHAR(1)) AS `DEBIT_CREDIT_INDICATOR`,
CAST (`representments`.`heritage` AS VARCHAR(3)) AS `HERITAGE`,
CAST (regexp_replace(`representments`.`gti`,' ','') AS BIGINT) AS `GTI`,
CAST (`representments`.`card_status` AS VARCHAR(12)) AS `CARD_STATUS`,
`representments`.`ingestion_year`,`representments`.`ingestion_month`,`representments`.`ingestion_day`
FROM RAW_BASE2_AL00242.representments;


#SCHEMA
create database if not exists SCHEMA_BASE2_AL00242;
DROP TABLE IF EXISTS SCHEMA_BASE2_AL00242.arbitrations;
CREATE EXTERNAL TABLE SCHEMA_BASE2_AL00242.arbitrations(
`record_id` varchar(9),
`reasoncode` int,
`amount` decimal(15,2),
`pan` bigint,
`transaction_entry_value_date` string,
`merchant` varchar(25),
`infopac_report_criteria` varchar(7),
`note` varchar(80),
`debit_credit_indicator` varchar(1),
`heritage` varchar(3),
`gti` bigint,
`edh_ingest_ts` timestamp,
`edh_ingest_delete_flag` boolean,
`edh_source_extract_ts` timestamp,
`edh_brand_id` varchar(3),
`edh_ingest_map` string)
PARTITIONED BY (
`ingestion_year` int,
`ingestion_month` int,
`ingestion_day` int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION 'gs://${hivevar:stagingBucket}/BASE2_AL00242/ARBITRATIONS/';



create database if not exists SCHEMA_BASE2_AL00242;
DROP TABLE IF EXISTS SCHEMA_BASE2_AL00242.chargebacks;
CREATE EXTERNAL TABLE SCHEMA_BASE2_AL00242.chargebacks(
`record_id` varchar(1),
`pan` bigint,
`amount` decimal(15,2),
`purchdate` string,
`reimbatt` int,
`arn` decimal(23,0),
`vrol_financial_id` varchar(12),
`merchant` varchar(25),
`city` varchar(13),
`trancode` int,
`entryvaluedate` string,
`heritagecode` varchar(4),
`debit_credit_indicator` varchar(1),
`heritage` varchar(3),
`gti` bigint,
`edh_ingest_ts` timestamp,
`edh_ingest_delete_flag` boolean,
`edh_source_extract_ts` timestamp,
`edh_brand_id` varchar(3),
`edh_ingest_map` string)
PARTITIONED BY (
`ingestion_year` int,
`ingestion_month` int,
`ingestion_day` int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION 'gs://${hivevar:stagingBucket}/BASE2_AL00242/CHARGEBACKS/';



create database if not exists SCHEMA_BASE2_AL00242;
DROP TABLE IF EXISTS SCHEMA_BASE2_AL00242.fraud_control;
CREATE EXTERNAL TABLE SCHEMA_BASE2_AL00242.fraud_control(
`record_id` varchar(1),
`pan` bigint,
`amount` decimal(15,2),
`trantype` varchar(2),
`arn` decimal(23,0),
`reasoncode` int,
`authcode` varchar(6),
`merchant` varchar(25),
`city` varchar(13),
`termtrandate` string,
`postingdate` string,
`heritagecode` varchar(4),
`debit_credit_indicator` varchar(1),
`heritage` varchar(3),
`gti` bigint,
`edh_ingest_ts` timestamp,
`edh_ingest_delete_flag` boolean,
`edh_source_extract_ts` timestamp,
`edh_brand_id` varchar(3),
`edh_ingest_map` string)
PARTITIONED BY (
`ingestion_year` int,
`ingestion_month` int,
`ingestion_day` int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION 'gs://${hivevar:stagingBucket}/BASE2_AL00242/FRAUD_CONTROL/';


create database if not exists SCHEMA_BASE2_AL00242;
DROP TABLE IF EXISTS SCHEMA_BASE2_AL00242.representments;
CREATE EXTERNAL TABLE SCHEMA_BASE2_AL00242.representments(
`record_id` varchar(1),
`pan` bigint,
`amount` decimal(15,2),
`trantype` varchar(2),
`arn` decimal(23,0),
`reasoncode` int,
`authcode` varchar(6),
`merchant` varchar(25),
`city` varchar(13),
`termtrandate` string,
`postingdate` string,
`heritagecode` varchar(4),
`debit_credit_indicator` varchar(1),
`heritage` varchar(3),
`gti` bigint,
`card_status` varchar(12),
`edh_ingest_ts` timestamp,
`edh_ingest_delete_flag` boolean,
`edh_source_extract_ts` timestamp,
`edh_brand_id` varchar(3),
`edh_ingest_map` string)
PARTITIONED BY (
`ingestion_year` int,
`ingestion_month` int,
`ingestion_day` int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION 'gs://${hivevar:stagingBucket}/BASE2_AL00242/REPRESENTMENTS/';


#Recon table
create database if not exists schema_base2_al00242;
Drop TABLE IF EXISTS schema_base2_al00242.recon_table;
CREATE EXTERNAL TABLE IF NOT EXISTS schema_base2_al00242.recon_table (
File_Name string,
File_Count string,
Cntrl_file_name string,
cntrl_file_count string,
tableName string,
columnName string,
valueextracted string,
Duplicate_count string,
Duplication_Ratio string,
Raw_count string,
Schema_count string,
SchemaCheckSumValue string,
Status string,
Reason string,
Ingestion_time timestamp
)
PARTITIONED BY (ingestion_year int,ingestion_month int,ingestion_day int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION "gs://${hivevar:stagingBucket}/BASE2_AL00242/CONTROL_FILE/";
