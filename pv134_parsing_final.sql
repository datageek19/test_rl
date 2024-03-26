-- parsing pv134

-- WITH cte AS (
--     SELECT 
--     	identifier,
--         name,
--         CASE
--             WHEN INSTR(rawcontent, 'PV1') > 0 THEN SUBSTRING_INDEX(
--                 SUBSTRING(rawcontent, INSTR(rawcontent, 'PV1')),
--                 '|',
--                 4
--             )
--         END AS extracted_value
--     FROM iris_adt_ds_optum_care.enslib_hl7_message_parquet
--     WHERE name LIKE 'ADT%'
--         AND SUBSTRING(timecreated, 1, 10) >= '2023-07-15'
-- ),
-- after_pv1 AS (
--     SELECT 
--     	identifier,
--         name,
--         extracted_value,
--         SUBSTRING_INDEX(
--             SUBSTRING_INDEX(extracted_value, '|', 4),
--             '|',
--             -1
--         ) AS extracted_value2
--     FROM cte),
-- pv134_final AS (
-- 	SELECT 
-- 		identifier,
--         name,
--         extracted_value,
--         extracted_value2,
--         SUBSTRING_INDEX(
--             SUBSTRING_INDEX(extracted_value2, '|', 2),
--             '|',
--             -1
--         ) AS pv134_result,
--         row_number() over (partition by identifier) a
--     FROM after_pv1
-- ),
-- pv134_carrot as (
-- 	SELECT
-- 		identifier,
--         name,
--         extracted_value,
--         extracted_value2,
--         pv134_result,
-- 	SUBSTRING_INDEX(SUBSTRING_INDEX(pv134_result, '^', 4), '^', -1) AS pv134_carrot
-- 	FROM
-- 	your_table;
-- )
-- select 
-- 	identifier,
--     name,
--     extracted_value,
--     extracted_value2,
--     pv134_result,
--     pv134_carrot
-- from pv134_carrot
-- where a =1


-- @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
WITH cte AS (
    SELECT 
    	identifier,
        name,
        CASE
            WHEN INSTR(rawcontent, 'PV1') > 0 THEN SUBSTRING_INDEX(
                SUBSTRING(rawcontent, INSTR(rawcontent, 'PV1')),
                '|',
                4
            )
        END AS extracted_value
    FROM iris_adt_ds_optum_care.enslib_hl7_message_parquet
    WHERE name LIKE 'ADT%'
        AND SUBSTRING(timecreated, 1, 10) >= '2023-10-15'
    limit 100
),
after_pv1 AS (
    SELECT 
    	identifier,
        name,
        extracted_value,
        SUBSTRING_INDEX(
            SUBSTRING_INDEX(extracted_value, '|', 4),
            '|',
            -1
        ) AS extracted_value2
    FROM cte),
pv134_carrot_final as (
	SELECT
		identifier,
        name,
        extracted_value,
        extracted_value2,
		SUBSTRING_INDEX(SUBSTRING_INDEX(extracted_value2, '^', 4), '^', -1) AS pv134_carrot,
	 	row_number() over (partition by identifier) a
	FROM
	after_pv1
)
select 
	identifier,
    name,
    extracted_value,
    extracted_value2,
    pv134_carrot
from pv134_carrot_final
where a =1