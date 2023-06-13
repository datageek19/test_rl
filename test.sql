SELECT sdr_received_source, cvx_code, procedurecode, COUNT(*)  
FROM sdr_inv.covid_immunizationnew_vw
WHERE ((std_servicedate >= '2022-08-01') AND (sdr_received_source LIKE 'PHIL%') AND cvx_code in ('302', '230', '301', '300', '229') or procedurecode in ('91312', '91313', '91314', '91315', '91316', '91317'))
GROUP BY sdr_received_source, cvx_code, procedurecode;


-- ====================
SELECT date_format(date_sub(date, cast(date_format(date, 'u') as int) - 1), 'yyyy-MM-dd') AS week_start_date, COUNT(lab_id) AS lab_count
FROM your_table_name
GROUP BY date_sub(date, cast(date_format(date, 'u') as int) - 1)
ORDER BY week_start_date;

-- Total lab counts by month
SELECT date_format(date_sub(date, cast(date_format(date, 'd') as int) - 1), 'yyyy-MM-dd') AS month_start_date, COUNT(lab_id) AS lab_count
FROM your_table_name
GROUP BY date_sub(date, cast(date_format(date, 'd') as int) - 1)
ORDER BY month_start_date;


