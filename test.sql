SELECT sdr_received_source, cvx_code, procedurecode, COUNT(*)  
FROM sdr_inv.covid_immunizationnew_vw
WHERE ((std_servicedate >= '2022-08-01') AND (sdr_received_source LIKE 'PHIL%') AND cvx_code in ('302', '230', '301', '300', '229') or procedurecode in ('91312', '91313', '91314', '91315', '91316', '91317'))
GROUP BY sdr_received_source, cvx_code, procedurecode;



