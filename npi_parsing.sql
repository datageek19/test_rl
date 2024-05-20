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

-- &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
-- &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

-- ** solution 1 in snowsql
-- Assuming your HL7 message is stored in a column named 'hl7_message' in a table named 'hl7_table'
SELECT 
  hl7_message,
  SUBSTRING(
    SUBSTRING(hl7_message, POSITION('PV1' IN hl7_message)), 
    POSITION('NPI&' IN SUBSTRING(hl7_message, POSITION('PV1' IN hl7_message))) + 4, 
    POSITION('&' IN SUBSTRING(SUBSTRING(hl7_message, POSITION('PV1' IN hl7_message)), POSITION('NPI&' IN SUBSTRING(hl7_message, POSITION('PV1' IN hl7_message))) + 4)) - 1
  ) AS PV1_NPI,
  SUBSTRING(
    SUBSTRING(hl7_message, POSITION('PD1' IN hl7_message)), 
    POSITION('NPI&' IN SUBSTRING(hl7_message, POSITION('PD1' IN hl7_message))) + 4, 
    POSITION('&' IN SUBSTRING(SUBSTRING(hl7_message, POSITION('PD1' IN hl7_message)), POSITION('NPI&' IN SUBSTRING(hl7_message, POSITION('PD1' IN hl7_message))) + 4)) - 1
  ) AS PD1_NPI
FROM hl7_table;

-- ** solution 2 in snowsql
-- Assuming your HL7 message is stored in a column named 'hl7_message' in a table named 'hl7_table'
WITH segments AS (
  SELECT 
    hl7_message,
    SEQ,
    SPLIT_PART(hl7_message, '|', SEQ) AS segment
  FROM hl7_table, TABLE(GENERATOR(ROWCOUNT => 100)) -- Adjust the rowcount value to the maximum number of segments in your HL7 messages
),
fields AS (
  SELECT 
    hl7_message,
    SEQ,
    segment,
    SPLIT_PART(segment, '^', SEQ) AS field
  FROM segments, TABLE(GENERATOR(ROWCOUNT => 100)) -- Adjust the rowcount value to the maximum number of fields in your segments
),
components AS (
  SELECT 
    hl7_message,
    SEQ,
    field,
    SPLIT_PART(field, '&', SEQ) AS component
  FROM fields, TABLE(GENERATOR(ROWCOUNT => 10)) -- Adjust the rowcount value to the maximum number of components in your fields
)
SELECT 
  hl7_message,
  MAX(CASE WHEN LEFT(segment, 3) = 'PV1' AND LEFT(field, 4) = 'NPI&' THEN component END) AS PV1_NPI,
  MAX(CASE WHEN LEFT(segment, 3) = 'PD1' AND LEFT(field, 4) = 'NPI&' THEN component END) AS PD1_NPI
FROM components
GROUP BY hl7_message;

-- ** solution 3:

-- Assuming your HL7 message is stored in a column named 'hl7_message' in a table named 'hl7_table'
SELECT 
  hl7_message,
  REGEXP_SUBSTR(hl7_message, 'PV1[^|]*\\|[^|]*NPI&([^&]*)') AS PV1_NPI,
  REGEXP_SUBSTR(hl7_message, 'PD1[^|]*\\|[^|]*NPI&([^&]*)') AS PD1_NPI
FROM hl7_table;

-- solution 4:

-- Assuming your HL7 message is stored in a column named 'hl7_message' in a table named 'hl7_table'
SELECT 
  hl7_message,
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_SUBSTR(hl7_message, 'PV1[^|]*\\|[^|]*NPI&[^&]*'), 
      '.*NPI&', ''
    ),
    '&.*', ''
  ) AS PV1_NPI,
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_SUBSTR(hl7_message, 'PD1[^|]*\\|[^|]*NPI&[^&]*'), 
      '.*NPI&', ''
    ),
    '&.*', ''
  ) AS PD1_NPI
FROM hl7_table;


-- soluiont 5:

-- Assuming your HL7 message is stored in a column named 'hl7_message' in a table named 'hl7_table'
SELECT 
  hl7_message,
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_SUBSTR(hl7_message, 'PV1[^|]*\\|[^|]*NPI&[^&]*'), 
      '.*NPI&', ''
    ),
    '&.*', ''
  ) AS PV1_NPI,
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_SUBSTR(hl7_message, 'PD1[^|]*\\|[^|]*NPI&[^&]*'), 
      '.*NPI&', ''
    ),
    '&.*', ''
  ) AS PD1_NPI
FROM hl7_table;
