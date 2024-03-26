-- pv139 parsing final

WITH cte AS (
    SELECT 
    	identifier,
        name,
        CASE
            WHEN INSTR(rawcontent, 'PV1') > 0 THEN SUBSTRING_INDEX(
                SUBSTRING(rawcontent, INSTR(rawcontent, 'PV1')),
                '|',
                40
            )
        END AS extracted_value
    FROM iris_adt_ds_optum_care.enslib_hl7_message_parquet
    WHERE name LIKE 'ADT%'
        AND SUBSTRING(timecreated, 1, 10) >= '2023-07-15'
),
after_pv1 AS (
    SELECT 
    	identifier,
        name,
        SUBSTRING_INDEX(
            SUBSTRING_INDEX(extracted_value, '|', 40),
            '|',
            -1
        ) AS extracted_value2
    FROM cte),
pv139_final AS (
	SELECT 
		identifier,
        name,
        SUBSTRING_INDEX(
            SUBSTRING_INDEX(extracted_value2, '|', 2),
            '|',
            -1
        ) AS pv139_result,
        row_number() over (partition by identifier) a
    FROM after_pv1
)
select --COUNT(*) 
	identifier, name, pv139_result
from pv139_final
where a =1