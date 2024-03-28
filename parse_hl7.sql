-- solution 1
SELECT
  -- Parse out string at 3rd delimiter after MSH key word
  SPLIT_PART(message, '|', 3) AS MSH_3,
  -- Parse out string at 20th delimiter after MSH key word then parse out string at 1st caret sign after @MSH.3
  SPLIT_PART(SPLIT_PART(message, '|', 20), '^', 1) AS MSH_20_1,
  -- Parse out string at 20th delimiter after MSH key word then parse out string at 1st caret sign after @MSH.4
  SPLIT_PART(SPLIT_PART(message, '|', 20), '^', 2) AS MSH_20_2,
  -- Parse out string at 3rd, 4th delimiter after PD1 key word
  SPLIT_PART(REGEXP_SUBSTR(message, 'PD1\\|([^|]*\\|){2}([^|]*)'), '|', 3) AS PD1_3,
  SPLIT_PART(REGEXP_SUBSTR(message, 'PD1\\|([^|]*\\|){3}([^|]*)'), '|', 4) AS PD1_4,
  -- Parse out string at 1,2,3,4,6,37,39 delimiter after PV1 key word; also parse out string after 1st, 2nd caret sign after PV1.1.1, PV1.2.1, PV1.3.1
  SPLIT_PART(REGEXP_SUBSTR(message, 'PV1\\|([^|]*\\|){0}([^|]*)'), '|', 1) AS PV1_1,
  SPLIT_PART(REGEXP_SUBSTR(message, 'PV1\\|([^|]*\\|){1}([^|]*)'), '|', 2) AS PV1_2,
  SPLIT_PART(REGEXP_SUBSTR(message, 'PV1\\|([^|]*\\|){2}([^|]*)'), '|', 3) AS PV1_3,
  SPLIT_PART(REGEXP_SUBSTR(message, 'PV1\\|([^|]*\\|){3}([^|]*)'), '|', 4) AS PV1_4,
  SPLIT_PART(REGEXP_SUBSTR(message, 'PV1\\|([^|]*\\|){5}([^|]*)'), '|', 6) AS PV1_6,
  SPLIT_PART(REGEXP_SUBSTR(message, 'PV1\\|([^|]*\\|){36}([^|]*)'), '|', 37) AS PV1_37,
  SPLIT_PART(REGEXP_SUBSTR(message, 'PV1\\|([^|]*\\|){38}([^|]*)'), '|', 39) AS PV1_39,
  SPLIT_PART(SPLIT_PART(REGEXP_SUBSTR(message, 'PV1\\|([^|]*\\|){0}([^|]*)'), '|', 1), '^', 1) AS PV1_1_1,
  SPLIT_PART(SPLIT_PART(REGEXP_SUBSTR(message, 'PV1\\|([^|]*\\|){1}([^|]*)'), '|', 2), '^', 1) AS PV1_2_1,
  SPLIT_PART(SPLIT_PART(REGEXP_SUBSTR(message, 'PV1\\|([^|]*\\|){2}([^|]*)'), '|', 3), '^', 1) AS PV1_3_1
FROM hl7
