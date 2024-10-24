-- &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
-- &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
-- suggested optimized query
WITH EncounterData AS (
    SELECT 
        T1.ENCOUNTER_ID,
        T1.PTNT_REF,
        T1.SERVICE_PROVIDER_ORG_REF,
        T1.TYP_CDNG_C,
        TRY_CAST(T1.PERIOD_START AS TIMESTAMP) AS PERIOD_START,
        TRY_CAST(T1.PERIOD_END AS TIMESTAMP) AS PERIOD_END,
        T1.RSN_CD1_CDNG_T,
        T1.HOSP_DSCHRG_DISP_CDNG_C,
        T1.ENCOUNTER_CLASS_CDNG_C
    FROM FOUNDATION.ENCOUNTER T1
    WHERE LEFT(T1.PERIOD_END, 10) IN ('2024-01-10', '2024-01-09', '2024-01-08', '2024-01-07', '2024-01-06')
    AND T1.ECDH_RECEIVED_SOURCE_CD = 'ADT'
    AND (T1.ENCOUNTER_CLASS_CDNG_C = 'EMER' OR T1.ENCOUNTER_CLASS_CDNG_D = 'OBSERVATION')
),

PatientData AS (
    SELECT 
        T2.PTNT_ID,
        LEFT(T2.NAME_FAMILY, 132) AS NAME_FAMILY,
        T2.NAME_GIVEN_FIRST,
        T2.ADDRESS_STATE,
        T2.TELECOM_HOME_PHONE,
        TRY_CAST(T2.BIRTH_DATE AS DATE) AS BIRTH_DATE
    FROM FOUNDATION.PATIENT T2
),

OrganizationData AS (
    SELECT 
        T3.ORG_ID,
        LEFT(SUBSTR(T3.ECDH_ROWKEY, CHARINDEX(':', T3.ECDH_ROWKEY) + 1), 
        CHARINDEX(':', SUBSTR(T3.ECDH_ROWKEY, CHARINDEX(':', T3.ECDH_ROWKEY) + 1)) - 1) AS AdmitHospital,
        LEFT(T3.ECDH_ROWKEY, CHARINDEX(':', T3.ECDH_ROWKEY) - 1) AS AdmitHospitalNPI
    FROM FOUNDATION.ORGANIZATION T3
),

DiagnosisRelatorData AS (
    SELECT 
        T4.ENCOUNTER_ID,
        T4.Rank AS Diag_Rank,
        T4.DIAGNOSIS_CONDITION_REF
    FROM FOUNDATION.ENCOUNTER_DIAGNOSIS_RELATOR T4
),

ConditionData AS (
    SELECT 
        T5.CONDITION_ID,
        T5.CD_CDNG_C AS Diag_CD,
        T5.CD_CDNG_D AS Diag_Desc
    FROM FOUNDATION.CONDITION T5
),

IdentifierData AS (
    SELECT 
        T6.Resource_ID,
        T6.Value AS MedicaidNumber
    FROM FOUNDATION.IDENTIFIER T6
    WHERE T6.Typ_Cdng_T = 'SMART Medicaid Number'
)

SELECT 
    ED.NAME_FAMILY,
    ED.NAME_GIVEN_FIRST,
    ED.ADDRESS_STATE,
    ED.TELECOM_HOME_PHONE,
    ED.BIRTH_DATE,
    Encounter.TYP_CDNG_C,
    Encounter.PERIOD_START,
    Organization.AdmitHospital,
    Organization.AdmitHospitalNPI,
    Encounter.RSN_CD1_CDNG_T,
    Encounter.PERIOD_END,
    Encounter.HOSP_DSCHRG_DISP_CDNG_C,
    DiagnosisRelator.Diag_Rank,
    Condition.Diag_CD,
    Condition.Diag_Desc,
    Encounter.ENCOUNTER_CLASS_CDNG_C,
    Identifier.MedicaidNumber,
    ROW_NUMBER() OVER (PARTITION BY ED.NAME_FAMILY, ED.NAME_GIVEN_FIRST, ED.BIRTH_DATE 
        ORDER BY ED.NAME_FAMILY, ED.NAME_GIVEN_FIRST, ED.BIRTH_DATE, Condition.Diag_CD) AS DiagRank2
FROM EncounterData Encounter
JOIN PatientData ED ON Encounter.PTNT_REF = ED.PTNT_ID
LEFT JOIN OrganizationData Organization ON Encounter.SERVICE_PROVIDER_ORG_REF = Organization.ORG_ID
LEFT JOIN DiagnosisRelatorData DiagnosisRelator ON Encounter.ENCOUNTER_ID = DiagnosisRelator.ENCOUNTER_ID
LEFT JOIN ConditionData Condition ON DiagnosisRelator.DIAGNOSIS_CONDITION_REF = Condition.CONDITION_ID
LEFT JOIN IdentifierData Identifier ON Encounter.PTNT_REF = Identifier.Resource_ID;

-- &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
-- &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
-- another optimized version of sas2ecdh
WITH EncounterData AS (
    SELECT 
        T1.ENCOUNTER_ID,
        T1.PTNT_REF,
        T1.SERVICE_PROVIDER_ORG_REF,
        T1.TYP_CDNG_C,
        TRY_CAST(T1.PERIOD_START AS TIMESTAMP) AS PERIOD_START,
        TRY_CAST(T1.PERIOD_END AS TIMESTAMP) AS PERIOD_END,
        T1.RSN_CD1_CDNG_T,
        T1.HOSP_DSCHRG_DISP_CDNG_C,
        T1.ENCOUNTER_CLASS_CDNG_C
    FROM FOUNDATION.ENCOUNTER T1
    WHERE LEFT(T1.PERIOD_END, 10) IN ('2024-01-10', '2024-01-09', '2024-01-08', '2024-01-07', '2024-01-06')
      AND T1.ECDH_RECEIVED_SOURCE_CD = 'ADT'
      AND (T1.ENCOUNTER_CLASS_CDNG_C = 'EMER' OR T1.ENCOUNTER_CLASS_CDNG_D = 'OBSERVATION')
),

PatientData AS (
    SELECT 
        T2.PTNT_ID,
        LEFT(T2.NAME_FAMILY, 132) AS NAME_FAMILY,
        T2.NAME_GIVEN_FIRST,
        T2.ADDRESS_STATE,
        T2.TELECOM_HOME_PHONE,
        TRY_CAST(T2.BIRTH_DATE AS DATE) AS BIRTH_DATE
    FROM FOUNDATION.PATIENT T2
),

OrganizationData AS (
    SELECT 
        T3.ORG_ID,
        LEFT(SUBSTR(T3.ECDH_ROWKEY, CHARINDEX(':', T3.ECDH_ROWKEY) + 1), 
        CHARINDEX(':', SUBSTR(T3.ECDH_ROWKEY, CHARINDEX(':', T3.ECDH_ROWKEY) + 1)) - 1) AS AdmitHospital,
        LEFT(T3.ECDH_ROWKEY, CHARINDEX(':', T3.ECDH_ROWKEY) - 1) AS AdmitHospitalNPI
    FROM FOUNDATION.ORGANIZATION T3
),

DiagnosisRelatorData AS (
    SELECT 
        T4.ENCOUNTER_ID,
        T4.Rank AS Diag_Rank,
        T4.DIAGNOSIS_CONDITION_REF
    FROM FOUNDATION.ENCOUNTER_DIAGNOSIS_RELATOR T4
),

ConditionData AS (
    SELECT 
        T5.CONDITION_ID,
        T5.CD_CDNG_C AS Diag_CD,
        T5.CD_CDNG_D AS Diag_Desc
    FROM FOUNDATION.CONDITION T5
),

IdentifierData AS (
    SELECT 
        T6.Resource_ID,
        T6.Value AS MedicaidNumber
    FROM FOUNDATION.IDENTIFIER T6
    WHERE T6.Typ_Cdng_T = 'SMART Medicaid Number'
)

SELECT 
    ED.NAME_FAMILY,
    ED.NAME_GIVEN_FIRST,
    ED.ADDRESS_STATE,
    ED.TELECOM_HOME_PHONE,
    ED.BIRTH_DATE,
    Encounter.TYP_CDNG_C,
    Encounter.PERIOD_START,
    Organization.AdmitHospital,
    Organization.AdmitHospitalNPI,
    Encounter.RSN_CD1_CDNG_T,
    Encounter.PERIOD_END,
    Encounter.HOSP_DSCHRG_DISP_CDNG_C,
    DiagnosisRelator.Diag_Rank,
    Condition.Diag_CD,
    Condition.Diag_Desc,
    Encounter.ENCOUNTER_CLASS_CDNG_C,
    Identifier.MedicaidNumber,
    ROW_NUMBER() OVER (PARTITION BY ED.NAME_FAMILY, ED.NAME_GIVEN_FIRST, ED.BIRTH_DATE 
        ORDER BY ED.NAME_FAMILY, ED.NAME_GIVEN_FIRST, ED.BIRTH_DATE, Condition.Diag_CD) AS DiagRank2
FROM EncounterData Encounter
JOIN PatientData ED ON Encounter.PTNT_REF = ED.PTNT_ID
LEFT JOIN OrganizationData Organization ON Encounter.SERVICE_PROVIDER_ORG_REF = Organization.ORG_ID
LEFT JOIN DiagnosisRelatorData DiagnosisRelator ON Encounter.ENCOUNTER_ID = DiagnosisRelator.ENCOUNTER_ID
LEFT JOIN ConditionData Condition ON DiagnosisRelator.DIAGNOSIS_CONDITION_REF = Condition.CONDITION_ID
LEFT JOIN IdentifierData Identifier ON Encounter.PTNT_REF = Identifier.Resource_ID;

-- &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
-- &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
-- optimized ecdhmod query

WITH cte AS (
    SELECT 
        P.FHIR_NAME,
        ARRAY_SIZE(P.FHIR_NAME) AS PATIENT_NAME_COUNT,
        (N.INDEX + 1) AS PATIENT_NAME_SEQUENCE,
        UPPER(N.VALUE:use::string) AS PATIENT_NAME_USE,
        UPPER(N.VALUE:text::string) AS PATIENT_NAME_TEXT,
        UPPER(N.VALUE:suffix[0]::string) AS PATIENT_SUFFIX,
        UPPER(N.VALUE:prefix[0]::string) AS PATIENT_PREFIX,
        UPPER(N.VALUE:family::string) AS NAME_FAMILY,
        UPPER(N.VALUE:given[0]::string) AS NAME_GIVEN_FIRST,
        UPPER(N.VALUE:given[1]::string) AS PATIENT_MIDDLE_NAME,
        p.FHIR_BIRTH_DATE AS BIRTH_DATE,
        p.ECDP_PATIENT_SURRG_ID,
        I.VALUE:type.coding[0].code::STRING AS PATIENT_IDENTIFIER_TYPE_CD,
        I.VALUE:type.coding[0].display::STRING AS PATIENT_IDENTIFIER_TYPE_DISPLAY,
        I.VALUE:value::string AS PATIENT_IDENTIFIER_TYPE_VALUE,
        UPPER(A.VALUE:state) AS ADDRESS_STATE,
        T.VALUE:value::string AS TELECOM_HOME_PHONE
    FROM
        ECT_PRD_ECDHMOD_DB.FOUNDATION.PATIENT_ADT p,
        LATERAL FLATTEN(INPUT => p.FHIR_NAME) N,
        LATERAL FLATTEN(INPUT => p.FHIR_IDENTIFIER) I,
        LATERAL FLATTEN(INPUT => p.FHIR_ADDRESS) A,
        LATERAL FLATTEN(INPUT => p.FHIR_TELECOM) T
    WHERE
        (A.INDEX + 1) = 1
        AND UPPER(T.VALUE:system::string) = 'TEL'
        AND UPPER(T.VALUE:use::string) = 'HOME'
),
cte2 AS (
    SELECT
        e.ECDP_PATIENT_SURRG_ID_REF,
        e.FHIR_SERVICE_PROVIDER,
        e.FHIR_CLASS,
        e.SUBMIT_NAME_REF,
        e.FHIR_PERIOD,
        ft.VALUE:coding[0].code::STRING AS TYP_CDNG_C,
        r.VALUE:coding[0].code::STRING AS RSN_CD_CDNG_C,
        r.VALUE:coding[0].display::STRING AS RSN_CD_CDNG_D,
        h.VALUE:coding[0].code::STRING AS HOSP_DSCHRG_DISP_CDNG_C,
        h.VALUE:coding[0].display::STRING AS HOSP_DSCHRG_DISP_CDNG_D,
        fd.VALUE:rank::STRING AS DIAG_RANK,
        fd.VALUE:condition.reference::STRING AS DIAG_REFERENCE
    FROM
        ECT_PRD_ECDHMOD_DB.FOUNDATION.ENCOUNTER_ADT e,
        LATERAL FLATTEN(INPUT => e.FHIR_TYPE) ft,
        LATERAL FLATTEN(INPUT => e.FHIR_REASON_CD) r,
        LATERAL FLATTEN(INPUT => e.FHIR_HOSPITALIZATION) h,
        LATERAL FLATTEN(INPUT => e.FHIR_DIAGNOSIS) fd
    WHERE
        (ft.INDEX + 1) = 1
        AND h.KEY = 'dischargeDisposition'
),
cte3 AS (
    SELECT
        o.FHIR_IDENTIFIER,
        o.SELECTED_FHIR_ORGANIZATION_RESOURCE_ID,
        i.VALUE:system::STRING AS ORGANIZATION_IDENTIFIER_SYSTEM,
        ARRAY_SIZE(o.FHIR_IDENTIFIER) AS ORGANIZATION_IDENTIFIER_COUNT,
        (i.INDEX + 1) AS ORGANIZATION_SEQUENCE,
        i.VALUE:type.coding[0].code::STRING AS ORGANIZATION_IDENTIFIER_TYPE_CD,
        i.VALUE:type.coding[0].display::STRING AS ORGANIZATION_IDENTIFIER_TYPE_DISPLAY,
        i.VALUE:type.coding[0].system::STRING AS ORGANIZATION_IDENTIFIER_TYPE_CODE_SYSTEM,
        i.VALUE:value::string AS ORGANIZATION_IDENTIFIER_TYPE_VALUE,
        o.FHIR_NAME AS ADMITHOSPITAL
    FROM
        ECT_PRD_ECDHMOD_DB.FOUNDATION.ORGANIZATION_ADT o,
        LATERAL FLATTEN(INPUT => o.FHIR_IDENTIFIER) i
    WHERE
        i.VALUE:type.coding[0].code::STRING = 'NPI'
),
cte4 AS (
    SELECT
        c.SELECTED_FHIR_CONDITION_RESOURCE_ID,
        fc2.VALUE:code::STRING AS DIAG_CD
    FROM
        ECT_PRD_ECDHMOD_DB.FOUNDATION.CONDITION_ADT c,
        LATERAL FLATTEN(INPUT => c.FHIR_CD) fc,
        LATERAL FLATTEN(INPUT => fc.VALUE) fc2
    WHERE
        (fc2.INDEX + 1) = '1'
),
cte5 AS (
    SELECT
        c.SELECTED_FHIR_CONDITION_RESOURCE_ID,
        fc.VALUE AS DIAG_DESC
    FROM
        ECT_PRD_ECDHMOD_DB.FOUNDATION.CONDITION_ADT c,
        LATERAL FLATTEN(INPUT => c.FHIR_CD) fc
    WHERE
        fc.KEY = 'text'
)
SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY NAME_FAMILY, NAME_GIVEN_FIRST, BIRTH_DATE
                       ORDER BY NAME_FAMILY, NAME_GIVEN_FIRST, BIRTH_DATE, DIAG_CD) AS DiagRank2
FROM (
    SELECT DISTINCT
        c1.NAME_FAMILY,
        c1.NAME_GIVEN_FIRST,
        c1.ADDRESS_STATE,
        c1.TELECOM_HOME_PHONE,
        c1.BIRTH_DATE,
        c2.TYP_CDNG_C,
        PARSE_JSON(c2.FHIR_PERIOD):"start"::STRING AS PERIOD_START,
        c3.ADMITHOSPITAL,
        c2.SUBMIT_NAME_REF,
        c3.ORGANIZATION_IDENTIFIER_TYPE_VALUE AS ADMITHOSPITALNPI,
        c2.RSN_CD_CDNG_C,
        c2.RSN_CD_CDNG_D,
        PARSE_JSON(c2.FHIR_PERIOD):"end"::STRING AS PERIOD_END,
        c2.HOSP_DSCHRG_DISP_CDNG_C,
        c2.HOSP_DSCHRG_DISP_CDNG_D,
        c2.DIAG_RANK,
        c4.DIAG_CD,
        c5.DIAG_DESC,
        UPPER(PARSE_JSON(c2.FHIR_CLASS):"code"::STRING) AS ENCOUNTER_CLASS_CDNG_C,
        c1.PATIENT_IDENTIFIER_TYPE_DISPLAY,
        c1.PATIENT_IDENTIFIER_TYPE_VALUE
    FROM
        cte2 c2
    JOIN cte c1 ON 
        PARSE_JSON(c2.FHIR_PERIOD):"end"::STRING >= TO_VARCHAR(CURRENT_TIMESTAMP(3) - INTERVAL '5 DAY')
        AND UPPER(PARSE_JSON(c2.FHIR_CLASS):"code"::STRING) IN ('EMER', 'OBSERVATION')  -- Adjusted to include both EMER and OBSERVATION
        AND c2.ECDP_PATIENT_SURRG_ID_REF = c1.ECDP_PATIENT_SURRG_ID
    LEFT JOIN cte3 c3 ON
        LTRIM(PARSE_JSON(c2.FHIR_SERVICE_PROVIDER):"reference"::STRING, 'Organization/') = c3.SELECTED_FHIR_ORGANIZATION_RESOURCE_ID
    LEFT JOIN cte4 c4 ON
        LTRIM(c2.DIAG_REFERENCE, 'Condition/') = c4.SELECTED_FHIR_CONDITION_RESOURCE_ID
    LEFT JOIN cte5 c5 ON
        c4.SELECTED_FHIR_CONDITION_RESOURCE_ID = c5.SELECTED_FHIR_CONDITION_RESOURCE_ID
)
ORDER BY
    NAME_FAMILY,
    NAME_GIVEN_FIRST,
    BIRTH_DATE,
    DiagRank2;

-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-- another vesion of optimized ecdhmod query
WITH cte AS (
    SELECT 
        P.FHIR_NAME,
        ARRAY_SIZE(P.FHIR_NAME) AS PATIENT_NAME_COUNT,
        (N.INDEX + 1) AS PATIENT_NAME_SEQUENCE,
        UPPER(N.VALUE:use::string) AS PATIENT_NAME_USE,
        UPPER(N.VALUE:text::string) AS PATIENT_NAME_TEXT,
        UPPER(N.VALUE:suffix[0]::string) AS PATIENT_SUFFIX,
        UPPER(N.VALUE:prefix[0]::string) AS PATIENT_PREFIX,
        UPPER(N.VALUE:family::string) AS NAME_FAMILY,
        UPPER(N.VALUE:given[0]::string) AS NAME_GIVEN_FIRST,
        UPPER(N.VALUE:given[1]::string) AS PATIENT_MIDDLE_NAME,
        p.FHIR_BIRTH_DATE AS BIRTH_DATE,
        p.ECDP_PATIENT_SURRG_ID,
        I.VALUE:type.coding[0].code::STRING AS PATIENT_IDENTIFIER_TYPE_CD,
        I.VALUE:type.coding[0].display::STRING AS PATIENT_IDENTIFIER_TYPE_DISPLAY,
        I.VALUE:value::string AS PATIENT_IDENTIFIER_TYPE_VALUE,
        UPPER(A.VALUE:state) AS ADDRESS_STATE,
        T.VALUE:value::string AS TELECOM_HOME_PHONE
    FROM
        ECT_PRD_ECDHMOD_DB.FOUNDATION.PATIENT_ADT p,
        LATERAL FLATTEN(INPUT => p.FHIR_NAME) N,
        LATERAL FLATTEN(INPUT => p.FHIR_IDENTIFIER) I,
        LATERAL FLATTEN(INPUT => p.FHIR_ADDRESS) A,
        LATERAL FLATTEN(INPUT => p.FHIR_TELECOM) T
    WHERE
        (A.INDEX + 1) = 1
        AND UPPER(T.VALUE:system::string) = 'TEL'
        AND UPPER(T.VALUE:use::string) = 'HOME'
),
cte2 AS (
    SELECT
        e.ECDP_PATIENT_SURRG_ID_REF,
        e.FHIR_SERVICE_PROVIDER,
        e.FHIR_CLASS,
        e.SUBMIT_NAME_REF,
        e.FHIR_PERIOD,
        ft.VALUE:coding[0].code::STRING AS TYP_CDNG_C,
        r.VALUE:coding[0].code::STRING AS RSN_CD_CDNG_C,
        r.VALUE:coding[0].display::STRING AS RSN_CD_CDNG_D,
        h.VALUE:coding[0].code::STRING AS HOSP_DSCHRG_DISP_CDNG_C,
        h.VALUE:coding[0].display::STRING AS HOSP_DSCHRG_DISP_CDNG_D,
        fd.VALUE:rank::STRING AS DIAG_RANK,
        fd.VALUE:condition.reference::STRING AS DIAG_REFERENCE
    FROM
        ECT_PRD_ECDHMOD_DB.FOUNDATION.ENCOUNTER_ADT e,
        LATERAL FLATTEN(INPUT => e.FHIR_TYPE) ft,
        LATERAL FLATTEN(INPUT => e.FHIR_REASON_CD) r,
        LATERAL FLATTEN(INPUT => e.FHIR_HOSPITALIZATION) h,
        LATERAL FLATTEN(INPUT => e.FHIR_DIAGNOSIS) fd
    WHERE
        (ft.INDEX + 1) = 1
        AND h.KEY = 'dischargeDisposition'
),
cte3 AS (
    SELECT
        o.FHIR_IDENTIFIER,
        o.SELECTED_FHIR_ORGANIZATION_RESOURCE_ID,
        i.VALUE:system::STRING AS ORGANIZATION_IDENTIFIER_SYSTEM,
        ARRAY_SIZE(o.FHIR_IDENTIFIER) AS ORGANIZATION_IDENTIFIER_COUNT,
        (i.INDEX + 1) AS ORGANIZATION_SEQUENCE,
        i.VALUE:type.coding[0].code::STRING AS ORGANIZATION_IDENTIFIER_TYPE_CD,
        i.VALUE:type.coding[0].display::STRING AS ORGANIZATION_IDENTIFIER_TYPE_DISPLAY,
        i.VALUE:type.coding[0].system::STRING AS ORGANIZATION_IDENTIFIER_TYPE_CODE_SYSTEM,
        i.VALUE:value::string AS ORGANIZATION_IDENTIFIER_TYPE_VALUE,
        o.FHIR_NAME AS ADMITHOSPITAL
    FROM
        ECT_PRD_ECDHMOD_DB.FOUNDATION.ORGANIZATION_ADT o,
        LATERAL FLATTEN(INPUT => o.FHIR_IDENTIFIER) i
    WHERE
        i.VALUE:type.coding[0].code::STRING = 'NPI'
),
cte4 AS (
    SELECT
        c.SELECTED_FHIR_CONDITION_RESOURCE_ID,
        fc2.VALUE:code::STRING AS DIAG_CD
    FROM
        ECT_PRD_ECDHMOD_DB.FOUNDATION.CONDITION_ADT c,
        LATERAL FLATTEN(INPUT => c.FHIR_CD) fc,
        LATERAL FLATTEN(INPUT => fc.VALUE) fc2
    WHERE
        (fc2.INDEX + 1) = '1'
),
cte5 AS (
    SELECT
        c.SELECTED_FHIR_CONDITION_RESOURCE_ID,
        fc.VALUE AS DIAG_DESC
    FROM
        ECT_PRD_ECDHMOD_DB.FOUNDATION.CONDITION_ADT c,
        LATERAL FLATTEN(INPUT => c.FHIR_CD) fc
    WHERE
        fc.KEY = 'text'
)
SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY NAME_FAMILY, NAME_GIVEN_FIRST, BIRTH_DATE
                       ORDER BY NAME_FAMILY, NAME_GIVEN_FIRST, BIRTH_DATE, DIAG_CD) AS DiagRank2
FROM (
    SELECT DISTINCT
        c1.NAME_FAMILY,
        c1.NAME_GIVEN_FIRST,
        c1.ADDRESS_STATE,
        c1.TELECOM_HOME_PHONE,
        c1.BIRTH_DATE,
        c2.TYP_CDNG_C,
        PARSE_JSON(c2.FHIR_PERIOD):"start"::STRING AS PERIOD_START,
        c3.ADMITHOSPITAL,
        c2.SUBMIT_NAME_REF,
        c3.ORGANIZATION_IDENTIFIER_TYPE_VALUE AS ADMITHOSPITALNPI,
        c2.RSN_CD_CDNG_C,
        c2.RSN_CD_CDNG_D,
        PARSE_JSON(c2.FHIR_PERIOD):"end"::STRING AS PERIOD_END,
        c2.HOSP_DSCHRG_DISP_CDNG_C,
        c2.HOSP_DSCHRG_DISP_CDNG_D,
        c2.DIAG_RANK,
        c4.DIAG_CD,
        c5.DIAG_DESC,
        UPPER(PARSE_JSON(c2.FHIR_CLASS):"code"::STRING) AS ENCOUNTER_CLASS_CDNG_C,
        c1.PATIENT_IDENTIFIER_TYPE_DISPLAY,
        c1.PATIENT_IDENTIFIER_TYPE_VALUE
    FROM
        cte2 c2
    JOIN cte c1 ON 
        PARSE_JSON(c2.FHIR_PERIOD):"end"::STRING >= TO_VARCHAR(CURRENT_TIMESTAMP(3) - INTERVAL '5 day')
        AND UPPER(PARSE_JSON(c2.FHIR_CLASS):"code"::STRING) = 'EMER'
        AND c2.ECDP_PATIENT_SURRG_ID_REF = c1.ECDP_PATIENT_SURRG_ID
    LEFT JOIN cte3 c3 ON
        LTRIM(PARSE_JSON(c2.FHIR_SERVICE_PROVIDER):"reference"::STRING, 'Organization/') = c3.SELECTED_FHIR_ORGANIZATION_RESOURCE_ID
    LEFT JOIN cte4 c4 ON
        LTRIM(c2.DIAG_REFERENCE, 'Condition/') = c4.SELECTED_FHIR_CONDITION_RESOURCE_ID
    LEFT JOIN cte5 c5 ON
        c4.SELECTED_FHIR_CONDITION_RESOURCE_ID = c5.SELECTED_FHIR_CONDITION_RESOURCE_ID
)
ORDER BY
    NAME_FAMILY,
    NAME_GIVEN_FIRST,
    BIRTH_DATE,
    DiagRank2;

