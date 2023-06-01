SELECT ECDH_SUBMITTED_FILE_TYPE, ECDH_LOAD_DTM, AVG(dayily_cnt) AS avg_daily_cnt 
FROM (
    SELECT 
    	ECDH_SUBMITTED_FILE_TYPE, ECDH_LOAD_DTM,
		-- AVG(DATEDIFF(WEEK,ECDH_LOAD_DTM,ECDH_ETL_LOAD_DTM)) AS avg_date_diff, 
		COUNT(*) AS dayily_cnt
    FROM ECT_PRD_ECDH_DB.FOUNDATION.OBSERVATION
    WHERE  ECDH_SUBMITTED_FILE_TYPE ='WLDC' AND ECDH_LOAD_DTM >= '2022-08-01'
    GROUP BY ECDH_SUBMITTED_FILE_TYPE,ECDH_LOAD_DTM
) AS x
GROUP BY ECDH_SUBMITTED_FILE_TYPE, ECDH_LOAD_DTM

-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
SELECT 
    ECDH_SUBMITTED_FILE_TYPE, 
    AVG(adt_by_hour) AS avg_adt_cnt_hour,
    AVG(adt_by_day) AS avg_adt_cnt_day,
    AVG(adt_by_week) AS avg_adt_cnt_week
FROM (
    SELECT ECDH_SUBMITTED_FILE_TYPE, 

        COUNT(
            CASE 
                WHEN ECDH_SUBMITTED_FILE_TYPE ='WLDC' AND ECDH_LOAD_DTM = DATEADD(HOUR, DATEDIFF(HOUR, ECDH_LOAD_DTM, ECDH_ETL_LOAD_DTM), 0)
                    THEN 1 ELSE 0 END
            ) AS adt_by_hour,
        COUNT(
            CASE 
                WHEN ECDH_SUBMITTED_FILE_TYPE ='WLDC' AND ECDH_LOAD_DTM = DATEADD(DAY, DATEDIFF(DAY, ECDH_LOAD_DTM, ECDH_ETL_LOAD_DTM), 0)
                    THEN 1 ELSE 0 END
            ) AS adt_by_daily,
        COUNT(
            CASE 
                WHEN ECDH_LOAD_DTM >= DATEADD(WEEK, DATEDIFF(WEEK, ECDH_LOAD_DTM, ECDH_ETL_LOAD_DTM), 0)
                    AND  DATE <  DATEADD(WEEK, DATEDIFF(WEEK, ECDH_LOAD_DTM, ECDH_ETL_LOAD_DTM) + 1, 0)
                    THEN 1 ELSE 0 END
            ) AS adt_by_weekly
        -- COUNT(*) AS adt_cnt
    FROM   ECT_PRD_ECDH_DB.FOUNDATION.OBSERVATION
    WHERE  ECDH_SUBMITTED_FILE_TYPE ='WLDC' AND ECDH_LOAD_DTM >= '2022-08-01'
        AND DATE >= DATEADD(MONTH, DATEDIFF(MONTH, ECDH_LOAD_DTM, ECDH_ETL_LOAD_DTM), 0)
        AND DATE <  DATEADD(MONTH, DATEDIFF(MONTH, ECDH_LOAD_DTM, ECDH_ETL_LOAD_DTM) + 1, 0)
    GROUP BY ECDH_SUBMITTED_FILE_TYPE, ECDH_LOAD_DTM, ECDH_ETL_LOAD_DTM
) AS tb1

GROUP BY ECDH_SUBMITTED_FILE_TYPE, ECDH_LOAD_DTM, ECDH_ETL_LOAD_DTM


--------------------------------------------------------
# updated query

SELECT
  EXTRACT(HOUR FROM admitted_start_dtime) AS hour,
  file_type,
  COUNT(*) AS count
FROM adt_table
GROUP BY EXTRACT(HOUR FROM admitted_start_dtime), file_type;


SELECT
  DATE_TRUNC('week', admitted_start_dtime) AS week_start_date,
  file_type,
  COUNT(*) AS count
FROM adt_table
GROUP BY DATE_TRUNC('week', admitted_start_dtime), file_type;


SELECT
  DATE_TRUNC('month', admitted_start_dtime) AS month_start_date,
  file_type,
  COUNT(*) AS count
FROM adt_table
GROUP BY DATE_TRUNC('month', admitted_start_dtime), file_type;


##+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++##
SELECT 
  date,
  AVG(count) AS average_count
FROM (
  SELECT 
    DATE(admitted_start_dtime) AS date,
    file_type,
    COUNT(*) AS count
  FROM adt_table
  GROUP BY DATE(admitted_start_dtime), file_type
) subquery
GROUP BY date;

##+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++##

SELECT 
  DATE_TRUNC('week', date) AS week_start_date,
  AVG(average_count) AS average_count
FROM (
  SELECT 
    DATE(admitted_start_dtime) AS date,
    file_type,
    COUNT(*) AS count
  FROM adt_table
  GROUP BY DATE(admitted_start_dtime), file_type
) subquery
GROUP BY DATE_TRUNC('week', date);

##+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++##
SELECT 
  DATE_TRUNC('month', date) AS month_start_date,
  AVG(average_count) AS average_count
FROM (
  SELECT 
    DATE(admitted_start_dtime) AS date,
    file_type,
    COUNT(*) AS count
  FROM adt_table
  GROUP BY DATE(admitted_start_dtime), file_type
) subquery
GROUP BY DATE_TRUNC('month', date);
##+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++##
