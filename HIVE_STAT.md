```SQL
WITH NON_TABLE 
AS 
   (
    SELECT    
              TABLE_INFO.DB_LOCATION_URI                AS URL
            , TABLE_INFO.NAME                           AS NAME
            , TABLE_INFO.TBL_ID                         AS TBL_ID
	    , TABLE_INFO.TBL_NAME                       AS TBL_NAME
	    , SUM(TABLE_INFO.TABLE_IDX)                 AS TOTAL_TABLE
	    , SUM(TABLE_INFO.EXIST_TABLE)               AS EX_TABLE
	    , SUM(TABLE_INFO.COL_COUNT)                 AS TOTAL_COL
	    , SUM(TABLE_INFO.EXIST_COL)                 AS EX_COL
	    , SUM(TABLE_INFO.RECORD)                    AS RECORD
	    , SUM(TABLE_INFO.CAPACITY)                  AS CAPACITY
	    , SUM(TABLE_INFO.RAW_CAPACITY)              AS RAW_CAPACITY
	    , SUM(TABLE_INFO.ORC)                       AS ORC
      FROM
          (
             SELECT   
	          DB."DB_LOCATION_URI"                  AS DB_LOCATION_URI
                , DB."NAME"                             AS NAME
                , TBL."TBL_ID"                          AS TBL_ID
                , SD."SD_ID"
                , SD."CD_ID"
		, TBL."TBL_NAME"                        AS TBL_NAME
                , 1                                     AS TABLE_IDX
		, (SELECT CASE WHEN TP."PARAM_VALUE"::NUMERIC <= 0 
                               THEN 0 
		               ELSE 1               END AS PV
                     FROM  "TABLE_PARAMS" TP 
                    WHERE TP."TBL_ID" = TBL."TBL_ID" 
                      AND "PARAM_KEY" = 'numFiles')     AS EXIST_TABLE	
		, COL.COL_COUNT                         AS COL_COUNT  
                , (SELECT CASE WHEN TP."PARAM_VALUE"::NUMERIC <= 0 
                               THEN 0 
                               ELSE COL.COL_COUNT   END AS PV
                     FROM  "TABLE_PARAMS" TP 
                    WHERE TP."TBL_ID" = TBL."TBL_ID" 
                      AND "PARAM_KEY" = 'numFiles')     AS EXIST_COL
		, (SELECT CASE WHEN TP."PARAM_VALUE"::NUMERIC < 0  
                               THEN 0 
		               ELSE TP."PARAM_VALUE"::NUMERIC END AS PV
                     FROM  "TABLE_PARAMS" TP 
                    WHERE TP."TBL_ID" = TBL."TBL_ID" 
                      AND "PARAM_KEY" = 'numRows')      AS RECORD
		, (SELECT CASE WHEN TP."PARAM_VALUE"::NUMERIC < 0  
                               THEN 0 
		               ELSE TP."PARAM_VALUE"::NUMERIC END AS PV
                     FROM  "TABLE_PARAMS" TP 
                    WHERE TP."TBL_ID" = TBL."TBL_ID" 
                      AND "PARAM_KEY" = 'totalSize')    AS CAPACITY	
		, (SELECT CASE WHEN TP."PARAM_VALUE"::NUMERIC < 0  
                               THEN 0 
		               ELSE TP."PARAM_VALUE"::NUMERIC END AS PV
                     FROM  "TABLE_PARAMS" TP 
                    WHERE TP."TBL_ID" = TBL."TBL_ID" 
                      AND "PARAM_KEY" = 'rawDataSize')  AS RAW_CAPACITY	
		, (SELECT CASE WHEN TP."PARAM_VALUE"::NUMERIC > 0 
                                AND SD."OUTPUT_FORMAT" ~* 'orc' 
                               THEN 1 
                               ELSE 0                    END AS PV
                     FROM  "TABLE_PARAMS" TP 
                    WHERE TP."TBL_ID" = TBL."TBL_ID" 
                      AND "PARAM_KEY" = 'numFiles')     AS ORC
		FROM  "TBLS" TBL
                JOIN  "DBS"  DB   
                  ON  TBL."DB_ID" = DB."DB_ID"  --{ AND DB."DB_LOCATION_URI" LIKE '%{}%'}
                JOIN  "SDS" SD 
				  ON  TBL."SD_ID" = SD."SD_ID"
                JOIN  (SELECT COL."CD_ID", COUNT(1) AS COL_COUNT FROM "COLUMNS_V2" COL GROUP BY COL."CD_ID") COL 
                  ON  SD."CD_ID" = COL."CD_ID"
               WHERE  "NAME" IN (SELECT ETL_LIST FROM ETL_SYSTEM_LIST)
	) TABLE_INFO
    GROUP BY TABLE_INFO."DB_LOCATION_URI", TABLE_INFO."NAME", TABLE_INFO.TBL_ID, TABLE_INFO.TBL_NAME
   )  
, PART_TABLE
AS 
   (
     SELECT          
	           PART_INFO.TBL_ID                          AS TBL_ID
	         , PART_INFO.TBL_NAME                        AS TBL_NAME
		 , SUM(PART_INFO.RECORD)                     AS RECORD
                 , MAX(PART.EX_COL)                          AS EX_COL
	         , SUM(PART.CAPACITY)                        AS CAPACITY
		 , SUM(PART.RAW_CAPACITY)                    AS RAW_CAPACITY
		 , 1                                         AS PART_
		 , MAX(ORC)                                  AS ORC
        FROM
            ( SELECT 
                   DB.DB_ID			
	         , TBL."TBL_ID"
	         , TBL."TBL_NAME"
	         , (SELECT CASE WHEN PTP."PARAM_VALUE"::NUMERIC < 0 
                                THEN 0 
                                ELSE COL.COL_COUNT END                      AS PV
                      FROM "PARTITION_PARAMS" PTP 
                     WHERE PTP."PART_ID" = PT."PART_ID" 
                       AND "PARAM_KEY" = 'numFiles')                        AS EX_COL
                 , (SELECT CASE WHEN PTP."PARAM_VALUE"::NUMERIC < 0 
                                THEN 0 ELSE PTP."PARAM_VALUE"::NUMERIC  END AS PV
		                 FROM "PARTITION_PARAMS" PTP 
                                WHERE PTP."PART_ID" = PT."PART_ID" 
                                  AND "PARAM_KEY" = 'numRows')               AS RECORD
	         , (SELECT CASE WHEN PTP."PARAM_VALUE"::NUMERIC < 0 
                                THEN 0 
                                ELSE PTP."PARAM_VALUE"::NUMERIC          END AS PV
		                FROM "PARTITION_PARAMS" PTP 
                     WHERE PTP."PART_ID" = PT."PART_ID" 
                       AND "PARAM_KEY" = 'totalSize')                        AS CAPACITY
                , (SELECT CASE WHEN PTP."PARAM_VALUE"::NUMERIC < 0 
                               THEN 0 
                               ELSE PTP."PARAM_VALUE"::NUMERIC            END AS PV
                     FROM "PARTITION_PARAMS" PTP 
                     WHERE PTP."PART_ID" = PT."PART_ID" 
                       AND "PARAM_KEY" = 'rawDataSize')                       AS RAW_CAPACITY
                 , (SELECT CASE WHEN PTP."PARTITION_PARAMS"::NUMERIC > 0 
                       AND SD."OUTPUT_FORMAT" ~* 'orc' 
                      THEN 1  
                      ELSE 0                                  END AS PV
                      FROM  "PARTITION_PARAMS" PTP 
                     WHERE PTP."PART_ID" = PT."PART_ID" 
                       AND "PARAM_KEY" = 'numFiles')              AS ORC				             
		  , PT."PART_ID"
	          , PT."PART_NAME"
		  , SD."LOCATION"
		  , SD."INPUT_FORMAT"
		  , SD."OUTPUT_FORMAT"
	       FROM   "TBLS" TBL 
	       JOIN "PARTITIONS" PT 
                 ON TBL."TBL_ID" = PT."TBL_ID"
	       JOIN "DBS" DB
                 ON TBL."DB_ID" = DB."DB_ID"
               JOIN "SDS" SD
                 ON TBL."SD_ID" = SD."SD_ID"
	       JOIN ( SELECT COL."CD_ID", COUNT(*) AS COL_COUNT 
                        FROM "COLUMNS_V2" COL 
                       GROUP BY COL."CD_ID") COL
                 ON SD."CD_ID" = COL."CD_ID"
	      WHERE DB."DB_LOCATION_URI" LIKE '%{}%'
	        AND DB."NAME" IN ( SELECT ETL_LIST FROM ETL_SYSTEM_LIST )
	 ) PART_INFO
     GROUP BY PART_INFO."TBL_ID", PART_INFO."TBL_NAME"
    )
SELECT 
          REGEXP_REPLACE(DB_.URL, 'hdfs://{}','') AS URL
	, DB_.NAME
	, SUM(DB_.TOTAL_TABLE)                    AS TOTAL_TABLE
	, SUM(DB_.EX_TABLE) + CASE WHEN SUM(DB_.PART_) IS NULL THEN 0 ELSE SUM(DB_.PART) END AS EX_TABLE
	, SUM(DB_.TOTAL_COL)                      AS TOTAL_COL
	, SUM(DB_.EX_COL)                         AS EX_COL
	, SUM(DB_.RECORD)                         AS RECORD     
        , SUM(DB_.CAPACITY)                       AS CAPACITY
 	, SUM(DB_.RAW_CAPACITY)                   AS RAW_CAPACITY
	, SUM(DB_.ORC)                            AS ORC
	, SUM(DB_.PART)                           AS PARTITION_
 FROM
      ( SELECT 
              NT.URL
           ,  NT.NAME
           ,  NT.TBL_ID
           ,  NT.TBL_NAME
           ,  NT.TOTAL_TABLE
           ,  NT.EX_TABLE
           ,  NT.TOTAL_COL
           , CASE WHEN NT.EX_TABLE IS NULL 
                  THEN PT.EX_COL       
                  ELSE NT.EX_COL END AS EX_COL
           , CASE WHEN NT.EX_TABLE IS NULL 
                  THEN PT.RECORD  
                  ELSE NT.RECORD END AS RECORD
           , CASE WHEN NT.EX_TABLE IS NULL 
                  THEN PT.CAPACITY     
                  ELSE NT.CAPACITY END AS CAPACITY				  
	   , CASE WHEN NT.EX_TABLE IS NULL 
                  THEN PT.RAW_CAPACITY ELSE NT.RAW_CAPACITY 
	   , CASE WHEN NT.EX_TABLE IS NULL 
                  THEN 0               ELSE NT.ORC END AS ORC 
	   , PT.PART_
	  FROM 
	      NON_TABLE NT 
  LEFT OUTER JOIN PART_TABLE PT
               ON NT.TBL_ID = PT.TBL_ID
       ) DB
GROUP BY DB_.URL, DB_.NAME
ORDER BY DB_.NAME
	
```

