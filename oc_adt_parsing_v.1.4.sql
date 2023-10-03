-- OC ADT parsing
-- @@ new round attmpt 1
SELECT
  SUBSTRING_INDEX(SUBSTRING_INDEX(rawcontent, '|', 40), '|', -1) AS extracted_value
FROM
  iris_adt_ds_optum_care.enslib_hl7_message_parquet
WHERE SUBSTRING(timecreated, 0,10) >= "2023-09-08" AND SUBSTRING(timecreated, 0,10) < "2023-09-09" LIMIT 20;

--@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
SELECT
  CASE
    WHEN INSTR(rawcontent, 'PV1') > 0
    THEN
      SUBSTRING_INDEX(
        SUBSTRING(rawcontent, INSTR(rawcontent, 'PV1')),
        '|', 40
      )
  END AS PV_RELATED_VALUES
FROM
  iris_adt_ds_optum_care.enslib_hl7_message_parquet
WHERE SUBSTRING(timecreated, 0,10) >= "2023-01-01" AND SUBSTRING(timecreated, 0,10) < "2023-01-07" LIMIT 20;

--@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

SELECT
  CASE
    WHEN INSTR(rawcontent, 'PV1') > 0
    THEN
      SUBSTRING(
        SUBSTRING_INDEX(
          rawcontent, '|', INSTR(rawcontent, 'PV1') + 38
        ),
        INSTR(
          SUBSTRING_INDEX(
            rawcontent, '|', INSTR(rawcontent, 'PV1') + 38
          ),
          '|'
        ) + 1
      )
  END AS PV1_39_VALUES
FROM
  iris_adt_ds_optum_care.enslib_hl7_message_parquet
WHERE SUBSTRING(timecreated, 0,10) >= "2023-01-01" AND SUBSTRING(timecreated, 0,10) < "2023-01-07";

-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



 SELECT
  CASE
    WHEN INSTR(rawcontent, 'PV1') > 0
    THEN
      REGEXP_EXTRACT(SUBSTRING(rawcontent, INSTR(rawcontent, 'PV1')), '^.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|.*?\\|(.*?)(\\||$)', 1)
  END AS extracted_value
FROM
  iris_adt_ds_optum_care.enslib_hl7_message_parquet
WHERE SUBSTRING(timecreated, 0,10) >= "2023-01-01" AND SUBSTRING(timecreated, 0,10) < "2023-01-07"
limit 1000;
  
-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
 SELECT
  CASE
    WHEN INSTR(rawcontent, 'PV1') > 0
    THEN
      SUBSTRING_INDEX(SUBSTRING(rawcontent, INSTR(rawcontent, 'PV1')), '|', 40)
  END AS extracted_value
FROM
  iris_adt_ds_optum_care.enslib_hl7_message_parquet
WHERE SUBSTRING(timecreated, 0,10) >= "2023-01-01" AND SUBSTRING(timecreated, 0,10) < "2023-01-07"
limit 10;


-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
 with cte as (
	SELECT
	  CASE
	    WHEN INSTR(rawcontent, 'PV1') > 0
	    THEN
	      SUBSTRING_INDEX(SUBSTRING(rawcontent, INSTR(rawcontent, 'PV1')), '|', 40)
	  END AS extracted_value
	FROM
	  iris_adt_ds_optum_care.enslib_hl7_message_parquet
	WHERE SUBSTRING(timecreated, 0,10) >= "2023-01-01" AND SUBSTRING(timecreated, 0,10) < "2023-01-07"
 )
SELECT
  	SUBSTRING_INDEX(SUBSTRING_INDEX(extracted_value, '|', 40), '|', -1) AS extracted_value2
FROM cte