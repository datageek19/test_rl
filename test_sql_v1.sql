WITH cte AS (
SELECT
	p.PTNT_ID,
	e.ENCOUNTER_CLASS_CDNG_C,
	e.SERVICE_TYP_CDNG_C,
	e.RSN_CD1_CDNG_T,
	e.EXTENSION_ADT_EVENT_TYPE AS "ADT Event Type",
	e.PERIOD_START AS "Admit Date",
	e.PERIOD_END AS "Discharge Date",
	e.HOSP_DSCHRG_DISP_CDNG_C,
	o.ORG_ID,
	o."NAME" AS "Facility Name",
	o.ADDRESS_T AS "Facility Address",
	o.ADDRESS_STATE AS "Facility State",
	e.ECDH_SUBMITTER_NAME,
	e.EXTENSION_ENTERED_AT_ORGANIZATION_REF,
	e.SERVICE_PROVIDER_ORG_REF,
	UPPER(p.NAME_GIVEN_FIRST) AS "Member First Name",
	UPPER(p.NAME_FAMILY) AS "Member Last Name",
	p.BIRTH_DATE AS "Member Birth Date",
	p.ADDRESS_T AS "Member Address",
	p.ADDRESS_STATE AS "Member State"
FROM
	ECT_PRD_ECDH_DB.FOUNDATION.ENCOUNTER e
JOIN ECT_PRD_ECDH_DB.FOUNDATION.ORGANIZATION o
ON
	e.ECDH_SUBMITTED_FILE_TYPE = 'ADT'
	AND UPPER(e.ECDH_SUBMITTER_NAME) = 'CDXTHATC' /*Name any submitter you would like to pull data for. May want to query distinct submitter names to find the one you are looking for.*/
/*Limit the data by specific timeline of admit and discharge dates*/
	AND ((substring(e.PERIOD_START, 0, 10) >= '2023-11-01'
		AND (substring(e.PERIOD_START, 0, 10) < '2024-02-01'))
		OR (substring(e.PERIOD_END, 0, 10) >= '2023-11-01'
			AND (substring(e.PERIOD_END, 0, 10) < '2024-02-01')))
	AND (e.EXTENSION_ENTERED_AT_ORGANIZATION_REF = o.ORG_ID
		OR e.SERVICE_PROVIDER_ORG_REF = o.ORG_ID)
JOIN ECT_PRD_ECDH_DB.FOUNDATION.PATIENT p
ON
	e.PTNT_REF = p.PTNT_ID
)
/*
,
cte2 AS (
SELECT
	UPPER(p.NAME_GIVEN_FIRST) AS "NAME_GIVEN_FIRST",
	UPPER(p.NAME_FAMILY) AS "NAME_FAMILY",
	SUBSTRING(p.BIRTH_DATE, 0, 10) AS "BIRTH_DATE",
	i.RESOURCE_ID,
	i.VALUE AS "EID",
	p.ADDRESS_T AS "Member Address",
	p.ADDRESS_STATE AS "Member State"
	ROW_NUMBER() OVER(PARTITION BY UPPER(p.NAME_GIVEN_FIRST), UPPER(p.NAME_FAMILY), SUBSTRING(p.BIRTH_DATE, 0, 10) ORDER BY i.ECDH_UPDATE_DTM DESC) AS N
FROM
	ECT_PRD_ECDH_DB.FOUNDATION.IDENTIFIER i
JOIN ECT_PRD_ECDH_DB.FOUNDATION.PATIENT p
ON
	i.RESOURCE_TYPE = 'Patient'
	AND (i.ASSIGNER_DISP = 'GR-EID:GR-EID'
	OR i.TYP_CDNG_T = 'iMDM Entity ID (EID)')
	AND i.RESOURCE_ID = p.PTNT_ID
)
,
cte3 AS (
SELECT 
	"EID",
	RESOURCE_ID,
	"NAME_GIVEN_FIRST",
	"NAME_FAMILY",
	"BIRTH_DATE",
	"Member Address",
	"Member State"
FROM
	cte
WHERE
	N = 1
)*/
/*
,
cte4 AS (
SELECT
	i.VALUE AS "SSN",
	p.PTNT_ID,
	UPPER(p.NAME_GIVEN_FIRST) AS "Member First Name",
	UPPER(p.NAME_FAMILY) AS "Member Last Name",
	p.BIRTH_DATE AS "Member Birth Date",
	p.ADDRESS_T AS "Member Address",
	p.ADDRESS_STATE AS "Member State"
FROM
	ECT_PRD_ECDH_DB.FOUNDATION.PATIENT p
JOIN ECT_PRD_ECDH_DB.FOUNDATION.IDENTIFIER i
ON
	i.RESOURCE_TYPE = 'Patient'
	AND i.TYP_CDNG_D = 'Social Security Number'
	AND p.PTNT_ID = i.RESOURCE_ID
GROUP BY
	i.VALUE,
	p.PTNT_ID,
	p.NAME_GIVEN_FIRST,
	p.NAME_FAMILY,
	p.BIRTH_DATE,
	p.ADDRESS_T,
	p.ADDRESS_STATE
)
*/
,
cte5 AS (
SELECT
	i.TYP_CDNG_D AS "Facility ID Type",
	i.VALUE AS "Facility ID Value",
	o.ORG_ID
FROM
	ECT_PRD_ECDH_DB.FOUNDATION.ORGANIZATION o
JOIN ECT_PRD_ECDH_DB.FOUNDATION.IDENTIFIER i
ON
	i.RESOURCE_TYPE = 'Organization'
	AND o.ORG_ID = i.RESOURCE_ID
)
,
cte6 AS (
SELECT
	/*c3."EID",*/
	c1."Member First Name",
	c1."Member Last Name",
	c1."Member Birth Date",
	c1."Member Address",
	c1."Member State",
	/*c4."SSN",*/
	c1.ENCOUNTER_CLASS_CDNG_C,
	c1.SERVICE_TYP_CDNG_C,
	c1.RSN_CD1_CDNG_T,
	c1."ADT Event Type",
	c1."Admit Date",
	c1."Discharge Date",
	c1.HOSP_DSCHRG_DISP_CDNG_C,
	c1."Facility Name",
	c1."Facility Address",
	c1."Facility State",
	c5."Facility ID Type",
	c5."Facility ID Value",
	c1.ECDH_SUBMITTER_NAME
FROM
	cte c1
/*JOIN cte3 c3
ON
	c1."Member First Name" = c3."Member First Name"
	AND c1."Member Last Name" = c3."Member Last Name"
	AND c1."Member Birth Date" = c3."Member Birth Date"*/
/*JOIN cte4 c4
ON
	c1."Member First Name" = c4."Member First Name"
	AND c1."Member Last Name" = c4."Member Last Name"
	AND c1."Member Birth Date" = c4."Member Birth Date"*/
JOIN cte5 c5
ON
	c1.EXTENSION_ENTERED_AT_ORGANIZATION_REF = c5.ORG_ID
	OR c1.SERVICE_PROVIDER_ORG_REF = c5.ORG_ID
GROUP BY
	/*c3."EID",*/
	c1."Member First Name",
	c1."Member Last Name",
	c1."Member Birth Date",
	c1."Member Address",
	c1."Member State", 
	/*c4."SSN",*/
	c1.ENCOUNTER_CLASS_CDNG_C,
	c1.SERVICE_TYP_CDNG_C,
	c1.RSN_CD1_CDNG_T,
	c1."ADT Event Type",
	c1."Admit Date",
	c1."Discharge Date",
	c1.HOSP_DSCHRG_DISP_CDNG_C,
	c1."Facility Name",
	c1."Facility Address",
	c1."Facility State",
	c5."Facility ID Type",
	c5."Facility ID Value",
	c1.ECDH_SUBMITTER_NAME
)
SELECT
	*
FROM
	cte6
PIVOT(MAX("Facility ID Value") FOR "Facility ID Type" IN ('Tax ID number', 'National provider identifier', 'Marketing Partner ID number'))
ORDER BY
	1,
	2,
	3,
	7,
	8
;
