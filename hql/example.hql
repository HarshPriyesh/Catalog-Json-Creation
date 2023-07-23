CREATE external table `consumption_rf_3_insfin_verona_mpf.reconciliationauditrecordopsmi`(
  `runDateTime` varchar(19),
  `batchName` varchar(50),
  `status` varchar(20),
  `ingestionDate` varchar(23),
  `apiName` varchar(50),
  `variableName` varchar(50),
  `groupByColumnName` varchar(100),
  `groupingValue` varchar(50),
  `statusMessage` String,
  `fnzValue` decimal(23,8),
  `currentViewValue` decimal(28,8)
)
PARTITIONED BY (runDate string)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS application_job_control(
  tivoliJobId string,
  lastprocessingtimestamp bigint,
  entityName string,
  lastprocessingstarttime string,
  lastprocessingendtime string,
  nextprocessingdate date,
  lastrunstatus string,
  sparkapplicationid string,
  duration string
  )
PARTITIONED BY(
   year string,
   month string
   )
STORED AS ORC;