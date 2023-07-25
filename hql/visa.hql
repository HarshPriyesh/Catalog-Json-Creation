create database if not exists RAW_BASE2_AL00242;
create database if not exists SCHEMA_BASE2_AL00242;

DROP TABLE IF EXISTS RAW_BASE2_AL00242.VISA_LBC;
CREATE EXTERNAL TABLE RAW_BASE2_AL00242.VISA_LBC(
NETWORK STRING,
EXCEPTION_TRANSACTION STRING,
SETTLE_DATE STRING,
DELIVERY_TIME STRING,
MEMBER_CASE STRING,
ROL_CASE STRING,
CARD_ACCOUNT_NUMBER STRING,
AMOUNT STRING,
DEBIT_CREDIT_INDICATOR STRING,
CURR_CODE STRING,
RC STRING,
TRAN_ID STRING,
ACQUIRER_REFFERENCE STRING,
MERCHANT_NAME STRING,
USER_NAME STRING,
TRN STRING,
JURISDICTION STRING,
ATM_INDICATOR STRING,
TOKEN STRING,
DISPUTE_CATEGORY STRING,
DISPUTE_CATEGORY_CONDITION STRING,
VROL_FINANCIAL_ID STRING,
VROL_BUNDLE_CASE STRING,
INTERCHANGE_FEE_AMOUNT STRING,
INTERCHANGE_FEE_DR_CR STRING,
INTERCHANGE_FEE_CURRENCY STRING,
DISPUTE_STATUS STRING,
DISPUTE_STATUS_DESCRIPTION STRING)
PARTITIONED BY(INGESTION_YEAR INT,INGESTION_MONTH INT,INGESTION_DAY INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'gs://${hivevar:rawBucket}/BASE2_AL00242/ROL500_01_LBCM/'
TBLPROPERTIES('serialization.null.format'='');


DROP VIEW IF EXISTS RAW_BASE2_AL00242.VISA_LBC_VW;
CREATE VIEW IF NOT EXISTS RAW_BASE2_AL00242.VISA_LBC_VW
AS SELECT
CAST (NETWORK AS STRING) AS NETWORK,
CAST (EXCEPTION_TRANSACTION AS STRING) AS EXCEPTION_TRANSACTION,
CAST (SETTLE_DATE AS STRING) as SETTLE_DATE,
CAST (DELIVERY_TIME AS STRING) AS DELIVERY_TIME,
CAST (MEMBER_CASE AS STRING) AS MEMBER_CASE,
CAST (ROL_CASE AS STRING) AS ROL_CASE,
CAST (CARD_ACCOUNT_NUMBER AS STRING) AS CARD_ACCOUNT_NUMBER,
CAST (regexp_replace(AMOUNT,',','') AS DECIMAL(8,2)) AS AMOUNT,
CAST (DEBIT_CREDIT_INDICATOR AS STRING) AS DEBIT_CREDIT_INDICATOR,
CAST (CURR_CODE AS INT) AS CURR_CODE,
CAST (RC AS STRING) AS RC,
CAST (TRAN_ID AS STRING) AS TRAN_ID,
CAST (ACQUIRER_REFFERENCE AS STRING) AS ACQUIRER_REFFERENCE,
CAST (MERCHANT_NAME AS STRING) AS MERCHANT_NAME,
CAST (USER_NAME AS STRING) AS USER_NAME,
CAST (TRN AS STRING) AS TRN,
CAST (JURISDICTION AS STRING) AS JURISDICTION,
CAST (ATM_INDICATOR AS STRING) AS ATM_INDICATOR,
CAST (TOKEN AS STRING) AS TOKEN,
CAST (DISPUTE_CATEGORY AS STRING) AS DISPUTE_CATEGORY,
CAST (DISPUTE_CATEGORY_CONDITION AS STRING) AS DISPUTE_CATEGORY_CONDITION,
CAST (VROL_FINANCIAL_ID AS STRING) AS VROL_FINANCIAL_ID,
CAST (VROL_BUNDLE_CASE AS STRING) AS VROL_BUNDLE_CASE,
CAST (INTERCHANGE_FEE_AMOUNT AS DECIMAL(7,6)) AS INTERCHANGE_FEE_AMOUNT,
CAST (INTERCHANGE_FEE_DR_CR AS STRING) AS INTERCHANGE_FEE_DR_CR,
CAST (INTERCHANGE_FEE_CURRENCY AS STRING) AS INTERCHANGE_FEE_CURRENCY,
CAST (DISPUTE_STATUS AS STRING) AS DISPUTE_STATUS,
CAST (DISPUTE_STATUS_DESCRIPTION AS STRING) AS DISPUTE_STATUS_DESCRIPTION,
INGESTION_YEAR,INGESTION_MONTH,INGESTION_DAY
FROM RAW_BASE2_AL00242.VISA_LBC;

DROP TABLE IF EXISTS SCHEMA_BASE2_AL00242.VISA_LBC;
CREATE EXTERNAL TABLE SCHEMA_BASE2_AL00242.VISA_LBC(
NETWORK STRING,
EXCEPTION_TRANSACTION STRING,
SETTLE_DATE STRING,
DELIVERY_TIME STRING,
MEMBER_CASE STRING,
ROL_CASE STRING,
CARD_ACCOUNT_NUMBER STRING,
AMOUNT DECIMAL(8,2),
DEBIT_CREDIT_INDICATOR STRING,
CURR_CODE INT,
RC STRING,
TRAN_ID STRING,
ACQUIRER_REFFERENCE STRING,
MERCHANT_NAME STRING,
USER_NAME STRING,
TRN STRING,
JURISDICTION STRING,
ATM_INDICATOR STRING,
TOKEN STRING,
DISPUTE_CATEGORY STRING,
DISPUTE_CATEGORY_CONDITION STRING,
VROL_FINANCIAL_ID STRING,
VROL_BUNDLE_CASE STRING,
INTERCHANGE_FEE_AMOUNT DECIMAL(7,6),
INTERCHANGE_FEE_DR_CR STRING,
INTERCHANGE_FEE_CURRENCY STRING,
DISPUTE_STATUS STRING,
DISPUTE_STATUS_DESCRIPTION STRING,
EDH_INGEST_TS TIMESTAMP,
EDH_INGEST_DELETE_FLAG BOOLEAN,
EDH_SOURCE_EXTRACT_TS TIMESTAMP,
EDH_BRAND_ID VARCHAR(3),
EDH_INGEST_MAP STRING)
PARTITIONED BY(INGESTION_YEAR INT,INGESTION_MONTH INT,INGESTION_DAY INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION 'gs://${hivevar:stagingBucket}/BASE2_AL00242/ROL500_01_LBIL/'
TBLPROPERTIES('serialization.null.format'='');


DROP TABLE IF EXISTS RAW_BASE2_AL00242.VISA_LOY;
CREATE EXTERNAL TABLE RAW_BASE2_AL00242.VISA_LOY(
NETWORK STRING,
EXCEPTION_TRANSACTION STRING,
SETTLE_DATE STRING,
DELIVERY_TIME STRING,
MEMBER_CASE STRING,
ROL_CASE STRING,
CARD_ACCOUNT_NUMBER STRING,
AMOUNT STRING,
DEBIT_CREDIT_INDICATOR STRING,
CURR_CODE STRING,
RC STRING,
TRAN_ID STRING,
ACQUIRER_REFFERENCE STRING,
MERCHANT_NAME STRING,
USER_NAME STRING,
TRN STRING,
JURISDICTION STRING,
ATM_INDICATOR STRING,
TOKEN STRING,
DISPUTE_CATEGORY STRING,
DISPUTE_CATEGORY_CONDITION STRING,
VROL_FINANCIAL_ID STRING,
VROL_BUNDLE_CASE STRING,
INTERCHANGE_FEE_AMOUNT STRING,
INTERCHANGE_FEE_DR_CR STRING,
INTERCHANGE_FEE_CURRENCY STRING,
DISPUTE_STATUS STRING,
DISPUTE_STATUS_DESCRIPTION STRING)
PARTITIONED BY(INGESTION_YEAR INT,INGESTION_MONTH INT,INGESTION_DAY INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'gs://${hivevar:rawBucket}/BASE2_AL00242/ROL500_01_LLOYDS/'
TBLPROPERTIES('serialization.null.format'='');


DROP VIEW IF EXISTS RAW_BASE2_AL00242.VISA_LOY_VW;
CREATE VIEW IF NOT EXISTS RAW_BASE2_AL00242.VISA_LOY_VW
AS SELECT
CAST (NETWORK AS STRING) AS NETWORK,
CAST (EXCEPTION_TRANSACTION AS STRING) AS EXCEPTION_TRANSACTION,
CAST (SETTLE_DATE AS STRING) as SETTLE_DATE,
CAST (DELIVERY_TIME AS STRING) AS DELIVERY_TIME,
CAST (MEMBER_CASE AS STRING) AS MEMBER_CASE,
CAST (ROL_CASE AS STRING) AS ROL_CASE,
CAST (CARD_ACCOUNT_NUMBER AS STRING) AS CARD_ACCOUNT_NUMBER,
CAST (regexp_replace(AMOUNT,',','') AS DECIMAL(8,2)) AS AMOUNT,
CAST (DEBIT_CREDIT_INDICATOR AS STRING) AS DEBIT_CREDIT_INDICATOR,
CAST (CURR_CODE AS INT) AS CURR_CODE,
CAST (RC AS STRING) AS RC,
CAST (TRAN_ID AS STRING) AS TRAN_ID,
CAST (ACQUIRER_REFFERENCE AS STRING) AS ACQUIRER_REFFERENCE,
CAST (MERCHANT_NAME AS STRING) AS MERCHANT_NAME,
CAST (USER_NAME AS STRING) AS USER_NAME,
CAST (TRN AS STRING) AS TRN,
CAST (JURISDICTION AS STRING) AS JURISDICTION,
CAST (ATM_INDICATOR AS STRING) AS ATM_INDICATOR,
CAST (TOKEN AS STRING) AS TOKEN,
CAST (DISPUTE_CATEGORY AS STRING) AS DISPUTE_CATEGORY,
CAST (DISPUTE_CATEGORY_CONDITION AS STRING) AS DISPUTE_CATEGORY_CONDITION,
CAST (VROL_FINANCIAL_ID AS STRING) AS VROL_FINANCIAL_ID,
CAST (VROL_BUNDLE_CASE AS STRING) AS VROL_BUNDLE_CASE,
CAST (INTERCHANGE_FEE_AMOUNT AS DECIMAL(7,6)) AS INTERCHANGE_FEE_AMOUNT,
CAST (INTERCHANGE_FEE_DR_CR AS STRING) AS INTERCHANGE_FEE_DR_CR,
CAST (INTERCHANGE_FEE_CURRENCY AS STRING) AS INTERCHANGE_FEE_CURRENCY,
CAST (DISPUTE_STATUS AS STRING) AS DISPUTE_STATUS,
CAST (DISPUTE_STATUS_DESCRIPTION AS STRING) AS DISPUTE_STATUS_DESCRIPTION,
INGESTION_YEAR,INGESTION_MONTH,INGESTION_DAY
FROM RAW_BASE2_AL00242.VISA_LOY;

DROP TABLE IF EXISTS SCHEMA_BASE2_AL00242.VISA_LOY;
CREATE EXTERNAL TABLE SCHEMA_BASE2_AL00242.VISA_LOY(
NETWORK STRING,
EXCEPTION_TRANSACTION STRING,
SETTLE_DATE STRING,
DELIVERY_TIME STRING,
MEMBER_CASE STRING,
ROL_CASE STRING,
CARD_ACCOUNT_NUMBER STRING,
AMOUNT DECIMAL(8,2),
DEBIT_CREDIT_INDICATOR STRING,
CURR_CODE INT,
RC STRING,
TRAN_ID STRING,
ACQUIRER_REFFERENCE STRING,
MERCHANT_NAME STRING,
USER_NAME STRING,
TRN STRING,
JURISDICTION STRING,
ATM_INDICATOR STRING,
TOKEN STRING,
DISPUTE_CATEGORY STRING,
DISPUTE_CATEGORY_CONDITION STRING,
VROL_FINANCIAL_ID STRING,
VROL_BUNDLE_CASE STRING,
INTERCHANGE_FEE_AMOUNT DECIMAL(7,6),
INTERCHANGE_FEE_DR_CR STRING,
INTERCHANGE_FEE_CURRENCY STRING,
DISPUTE_STATUS STRING,
DISPUTE_STATUS_DESCRIPTION STRING,
EDH_INGEST_TS TIMESTAMP,
EDH_INGEST_DELETE_FLAG BOOLEAN,
EDH_SOURCE_EXTRACT_TS TIMESTAMP,
EDH_BRAND_ID VARCHAR(3),
EDH_INGEST_MAP STRING)
PARTITIONED BY(INGESTION_YEAR INT,INGESTION_MONTH INT,INGESTION_DAY INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION 'gs://${hivevar:stagingBucket}/BASE2_AL00242/ROL500_01_LLOYDS/'
TBLPROPERTIES('serialization.null.format'='');


DROP TABLE IF EXISTS RAW_BASE2_AL00242.VISA_HBOS;
CREATE EXTERNAL TABLE RAW_BASE2_AL00242.VISA_HBOS(
NETWORK STRING,
EXCEPTION_TRANSACTION STRING,
SETTLE_DATE STRING,
DELIVERY_TIME STRING,
MEMBER_CASE STRING,
ROL_CASE STRING,
CARD_ACCOUNT_NUMBER STRING,
AMOUNT STRING,
DEBIT_CREDIT_INDICATOR STRING,
CURR_CODE STRING,
RC STRING,
TRAN_ID STRING,
ACQUIRER_REFFERENCE STRING,
MERCHANT_NAME STRING,
USER_NAME STRING,
TRN STRING,
JURISDICTION STRING,
ATM_INDICATOR STRING,
TOKEN STRING,
DISPUTE_CATEGORY STRING,
DISPUTE_CATEGORY_CONDITION STRING,
VROL_FINANCIAL_ID STRING,
VROL_BUNDLE_CASE STRING,
INTERCHANGE_FEE_AMOUNT STRING,
INTERCHANGE_FEE_DR_CR STRING,
INTERCHANGE_FEE_CURRENCY STRING,
DISPUTE_STATUS STRING,
DISPUTE_STATUS_DESCRIPTION STRING)
PARTITIONED BY(INGESTION_YEAR INT,INGESTION_MONTH INT,INGESTION_DAY INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'gs://${hivevar:rawBucket}/BASE2_AL00242/ROL500_01_HBOS/'
TBLPROPERTIES('serialization.null.format'='');


DROP VIEW IF EXISTS RAW_BASE2_AL00242.VISA_HBOS_VW;
CREATE VIEW IF NOT EXISTS RAW_BASE2_AL00242.VISA_HBOS_VW
AS SELECT
CAST (NETWORK AS STRING) AS NETWORK,
CAST (EXCEPTION_TRANSACTION AS STRING) AS EXCEPTION_TRANSACTION,
CAST (SETTLE_DATE AS STRING) as SETTLE_DATE,
CAST (DELIVERY_TIME AS STRING) AS DELIVERY_TIME,
CAST (MEMBER_CASE AS STRING) AS MEMBER_CASE,
CAST (ROL_CASE AS STRING) AS ROL_CASE,
CAST (CARD_ACCOUNT_NUMBER AS STRING) AS CARD_ACCOUNT_NUMBER,
CAST (regexp_replace(AMOUNT,',','') AS DECIMAL(8,2)) AS AMOUNT,
CAST (DEBIT_CREDIT_INDICATOR AS STRING) AS DEBIT_CREDIT_INDICATOR,
CAST (CURR_CODE AS INT) AS CURR_CODE,
CAST (RC AS STRING) AS RC,
CAST (TRAN_ID AS STRING) AS TRAN_ID,
CAST (ACQUIRER_REFFERENCE AS STRING) AS ACQUIRER_REFFERENCE,
CAST (MERCHANT_NAME AS STRING) AS MERCHANT_NAME,
CAST (USER_NAME AS STRING) AS USER_NAME,
CAST (TRN AS STRING) AS TRN,
CAST (JURISDICTION AS STRING) AS JURISDICTION,
CAST (ATM_INDICATOR AS STRING) AS ATM_INDICATOR,
CAST (TOKEN AS STRING) AS TOKEN,
CAST (DISPUTE_CATEGORY AS STRING) AS DISPUTE_CATEGORY,
CAST (DISPUTE_CATEGORY_CONDITION AS STRING) AS DISPUTE_CATEGORY_CONDITION,
CAST (VROL_FINANCIAL_ID AS STRING) AS VROL_FINANCIAL_ID,
CAST (VROL_BUNDLE_CASE AS STRING) AS VROL_BUNDLE_CASE,
CAST (INTERCHANGE_FEE_AMOUNT AS DECIMAL(7,6)) AS INTERCHANGE_FEE_AMOUNT,
CAST (INTERCHANGE_FEE_DR_CR AS STRING) AS INTERCHANGE_FEE_DR_CR,
CAST (INTERCHANGE_FEE_CURRENCY AS STRING) AS INTERCHANGE_FEE_CURRENCY,
CAST (DISPUTE_STATUS AS STRING) AS DISPUTE_STATUS,
CAST (DISPUTE_STATUS_DESCRIPTION AS STRING) AS DISPUTE_STATUS_DESCRIPTION,
INGESTION_YEAR,INGESTION_MONTH,INGESTION_DAY
FROM RAW_BASE2_AL00242.VISA_HBOS;

DROP TABLE IF EXISTS SCHEMA_BASE2_AL00242.VISA_HBOS;
CREATE EXTERNAL TABLE SCHEMA_BASE2_AL00242.VISA_HBOS(
NETWORK STRING,
EXCEPTION_TRANSACTION STRING,
SETTLE_DATE STRING,
DELIVERY_TIME STRING,
MEMBER_CASE STRING,
ROL_CASE STRING,
CARD_ACCOUNT_NUMBER STRING,
AMOUNT DECIMAL(8,2),
DEBIT_CREDIT_INDICATOR STRING,
CURR_CODE INT,
RC STRING,
TRAN_ID STRING,
ACQUIRER_REFFERENCE STRING,
MERCHANT_NAME STRING,
USER_NAME STRING,
TRN STRING,
JURISDICTION STRING,
ATM_INDICATOR STRING,
TOKEN STRING,
DISPUTE_CATEGORY STRING,
DISPUTE_CATEGORY_CONDITION STRING,
VROL_FINANCIAL_ID STRING,
VROL_BUNDLE_CASE STRING,
INTERCHANGE_FEE_AMOUNT DECIMAL(7,6),
INTERCHANGE_FEE_DR_CR STRING,
INTERCHANGE_FEE_CURRENCY STRING,
DISPUTE_STATUS STRING,
DISPUTE_STATUS_DESCRIPTION STRING,
EDH_INGEST_TS TIMESTAMP,
EDH_INGEST_DELETE_FLAG BOOLEAN,
EDH_SOURCE_EXTRACT_TS TIMESTAMP,
EDH_BRAND_ID VARCHAR(3),
EDH_INGEST_MAP STRING)
PARTITIONED BY(INGESTION_YEAR INT,INGESTION_MONTH INT,INGESTION_DAY INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION 'gs://${hivevar:stagingBucket}/BASE2_AL00242/ROL500_01_HBOS/'
TBLPROPERTIES('serialization.null.format'='');

#Recon table
create database if not exists schema_base2_al00242;
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
Ingestion_time timestamp)
PARTITIONED BY (ingestion_year int,ingestion_month int,ingestion_day int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION "gs://${hivevar:base2StagingBucket}/BASE2_AL00242/CONTROL_FILE/";