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
