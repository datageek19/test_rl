-- prase out npi values in PV1, PD1 segment
-- solution 1

SELECT 
    regexp_extract(column_name, 'PV1.*?NPI&([0-9]+)&', 1) AS PV1_NPI,
    regexp_extract(column_name, 'PD1.*?NPI&([0-9]+)&', 1) AS PD1_NPI
FROM 
    table_name

-- solution 2:

SELECT 
    regexp_extract(
        split(regexp_extract(hl7_message, 'PV1.*', 0), '\\|')[7], 
        'NPI&([0-9]+)&', 
        1
    ) AS PV1_NPI,
    regexp_extract(
        split(regexp_extract(hl7_message, 'PD1.*', 0), '\\|')[3], 
        'NPI&([0-9]+)&', 
        1
    ) AS PD1_NPI
FROM 
    table_name;

-- solution 3
SELECT 
    regexp_extract(regexp_extract(hl7_message, 'PV1.*', 0), 'NPI&([0-9]+)&', 1) AS PV1_NPI,
    regexp_extract(regexp_extract(hl7_message, 'PD1.*', 0), 'NPI&([0-9]+)&', 1) AS PD1_NPI
FROM 
    table_name;

-- solution 4
SELECT 
    regexp_replace(regexp_extract(hl7_message, 'PV1.*', 0), '[^0-9]', '') AS PV1_NPI,
    regexp_replace(regexp_extract(hl7_message, 'PD1.*', 0), '[^0-9]', '') AS PD1_NPI
FROM 
    table_name;

-- solution 5
SELECT 
    regexp_extract(hl7_message, 'PV1\\|.*?NPI&([0-9]+)&', 1) AS PV1_NPI,
    regexp_extract(hl7_message, 'PD1\\|.*?NPI&([0-9]+)&', 1) AS PD1_NPI
FROM 
    table_name;

-- solution 6
SELECT 
    regexp_extract(hl7_message, 'PV1\\|.*?NPI&([0-9]+)&', 1) AS PV1_NPI,
    regexp_extract(hl7_message, 'PD1\\|.*?NPI&([0-9]+)&', 1) AS PD1_NPI
FROM 
    table_name;

-- solution 7
SELECT 
    regexp_extract(hl7_message, 'PV1\\|.*?NPI&([0-9]+)&', 1) AS PV1_NPI,
    regexp_extract(hl7_message, 'PD1\\|.*?NPI&([0-9]+)&', 1) AS PD1_NPI
FROM 
    table_name;
