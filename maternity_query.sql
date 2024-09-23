--CONTAINS

--scenario 1
/*
Get patient data that contains drug keywords AND maternity cases. This query results in a lower count since it is first filtering on drug keywords
and then from that table, selects the data that also contains maternity keywords. This functions like AND statements
*/
with cte as (
	SELECT --get base ptnt ER data for TN
		e.RSN_CD1_CDNG_T, --some times this and the cd_cdng_c will be different what the dr says
		c.cd_cdng_c, --the actual doctor inputed code
		c.cd_cdng_t, --compare this to to cd_cdng_c
		e.ptnt_ref,
		e.PERIOD_START,
		p.address_state,
		e.PERIOD_END, 
		e.ENCOUNTER_ID,
		e.ECDH_SUBMITTED_FILE_TYPE,
		E.ECDH_SUBMITTER_NAME
	   from ECT_PRD_ECDH_DB.FOUNDATION.ENCOUNTER e
			inner join ECT_PRD_ECDH_DB.FOUNDATION.PATIENT p 
				on e.ptnt_ref = p.ptnt_id
			INNER JOIN ECT_PRD_ECDH_DB.FOUNDATION.condition AS C 
				ON E.encounter_id = c.encounter_ref
	where   (
			UPPER(e.ECDH_SUBMITTER_NAME) in ('CDXTHATC', 'EPP','HOSPITAL-CORP-OF-AMERICA','WTHC')  
			--OR p.ADDRESS_STATE ='TN'--potentially remove TN as a test
			) 
		-- AND P.ECDH_CS_LOB_FLAG = 'Y' --test to remove 
		and e.ECDH_SUBMITTED_FILE_TYPE ='ADT'
		-- and e.ENCOUNTER_CLASS_CDNG_C IN ('ERU', 'ER', 'EMERGENCY', 'EMER') 
    ),
cte2 as ( --get ptnt keyword data 
    select * 
    from cte as c1
        where (contains(UPPER(rsn_cd1_cdng_t),'ALCOHOL')
        OR contains(UPPER(rsn_cd1_cdng_t),'AMPHETAMINES')
        OR contains(UPPER(rsn_cd1_cdng_t),'AVINZA')
        OR contains(UPPER(rsn_cd1_cdng_t),'BARBITURATES')
        OR contains(UPPER(rsn_cd1_cdng_t),'BENZO ')
        OR contains(UPPER(rsn_cd1_cdng_t),'BENZODIAZEPINE')
        OR contains(UPPER(rsn_cd1_cdng_t),'BLOOD ALCOHOL')
        OR contains(UPPER(rsn_cd1_cdng_t),'BUPRENORPHINE')
        OR contains(UPPER(rsn_cd1_cdng_t),'CANNABI')
        OR contains(UPPER(rsn_cd1_cdng_t),'COCAINE')
        OR contains(UPPER(rsn_cd1_cdng_t),'CODEINE')
        OR contains(UPPER(rsn_cd1_cdng_t),'CRACK')
        OR contains(UPPER(rsn_cd1_cdng_t),'DETOX')
        OR contains(UPPER(rsn_cd1_cdng_t),'DILAUDID')
        OR contains(UPPER(rsn_cd1_cdng_t),'DRUG ABUSE')
        OR contains(UPPER(rsn_cd1_cdng_t),'DRUG DEPENDENCE')
        OR contains(UPPER(rsn_cd1_cdng_t),'DURAGESIC')
        OR contains(UPPER(rsn_cd1_cdng_t),'ETHANOL')
        OR contains(UPPER(rsn_cd1_cdng_t),'FENTANYL')
        OR contains(UPPER(rsn_cd1_cdng_t),'HALLUCINOGEN')
        OR contains(UPPER(rsn_cd1_cdng_t),'HEROIN')
        OR contains(UPPER(rsn_cd1_cdng_t),'HYDROCODONE')
        OR contains(UPPER(rsn_cd1_cdng_t),'HYPNOTIC')
        OR contains(UPPER(rsn_cd1_cdng_t),'INTOXICATION')
        OR contains(UPPER(rsn_cd1_cdng_t),'IVDU')
        OR contains(UPPER(rsn_cd1_cdng_t),'LSD')
        OR contains(UPPER(rsn_cd1_cdng_t),'METHADONE')
        OR contains(UPPER(rsn_cd1_cdng_t),'METHAMPHETAMINE')
        OR contains(UPPER(rsn_cd1_cdng_t),'METHYLPHENIDATE')
        OR contains(UPPER(rsn_cd1_cdng_t),'MORPHINE')
        OR contains(UPPER(rsn_cd1_cdng_t),'NALOXONE')
        OR contains(UPPER(rsn_cd1_cdng_t),'NALTREXONE')
        OR contains(UPPER(rsn_cd1_cdng_t),'NARCAN')
        OR contains(UPPER(rsn_cd1_cdng_t),'NARCOTIC')
        OR contains(UPPER(rsn_cd1_cdng_t),'O.D.')
        -- OR contains(UPPER(rsn_cd1_cdng_t),'OD')
        OR contains(UPPER(rsn_cd1_cdng_t),'OPANA')
        OR contains(UPPER(rsn_cd1_cdng_t),'OPIATE')
        OR contains(UPPER(rsn_cd1_cdng_t),'OPIOID')
        OR contains(UPPER(rsn_cd1_cdng_t),'OPIUM')
        OR contains(UPPER(rsn_cd1_cdng_t),'OUD)')
        OR contains(UPPER(rsn_cd1_cdng_t),'OVERDOSE')
        OR contains(UPPER(rsn_cd1_cdng_t),'OXYCODONE')
        OR contains(UPPER(rsn_cd1_cdng_t),'OXYCONTIN')
        OR contains(UPPER(rsn_cd1_cdng_t),'PERCOCET')
        OR contains(UPPER(rsn_cd1_cdng_t),'POLYDRUG')
        OR contains(UPPER(rsn_cd1_cdng_t),'POLYSUB')
        OR contains(UPPER(rsn_cd1_cdng_t),'PSUD')
        OR contains(UPPER(rsn_cd1_cdng_t),'PSYCHOACTIVE')
        OR contains(UPPER(rsn_cd1_cdng_t),'SEDATIVE')
        OR contains(UPPER(rsn_cd1_cdng_t),'STIMULANT')
        OR contains(UPPER(rsn_cd1_cdng_t),'SUBOXONE')
        OR contains(UPPER(rsn_cd1_cdng_t),'VICODIN')
        )
        or (c1.cd_cdng_c IN ('T40','F11', 'P96.1', 'T40.6', 'T40.60', 'T40.601', 'T40.601A', 'T40.601D', 'T40.601S', 'T40.602', 'T40.602A', 'T40.602D', 'T40.602S', 'T40.603', 'T40.603A', 'T40.603D', 'T40.603S', 'T40.604', 'T40.604A', 'T40.604D', 'T40.604S', 'T40.69', 'T40.691', 'T40.691A', 'T40.691D', 'T40.691S', 'T40.692', 'T40.692A', 'T40.692D', 'T40.692S', 'T40.693', 'T40.693A', 'T40.693D', 'T40.693S', 'T40.694', 'T40.694A', 'T40.694D', 'T40.694S', 'T40.', 'T40.0', 'T40.0X', 'T40.0X1', 'T40.0X1A', 'T40.0X1D', 'T40.0X1S', 'T40.0X2', 'T40.0X2A', 'T40.0X2D', 'T40.0X2S', 'T40.0X3', 'T40.0X3A', 'T40.0X3D', 'T40.0X3S', 'T40.0X4', 'T40.0X4A', 'T40.0X4D', 'T40.0X4S', 'T40.0X6', 'T40.0X6A', 'T40.0X6D', 'T40.0X6S', 'T40.1X1A', 'T40.1X1D', 'T40.1X4A', 'T40.1X4D', 'T40.1X4S', 'T40.2', 'T40.2X', 'T40.2X1', 'T40.2X1A', 'T40.2X1D', 'T40.2X1S', 'T40.2X2', 'T40.2X2A', 'T40.2X2D', 'T40.2X2S', 'T40.2X3', 'T40.2X3A', 'T40.2X3D', 'T40.2X3S', 'T40.2X4', 'T40.2X4A', 'T40.2X4D', 'T40.2X4S', 'T40.2X6', 'T40.2X6A', 'T40.2X6D', 'T40.2X6S', 'T40.3', 'T40.3X', 'T40.3X1', 'T40.3X1A', 'T40.3X1D', 'T40.3X1S', 'T40.3X2', 'T40.3X2A', 'T40.3X2D', 'T40.3X2S', 'T40.3X3', 'T40.3X3A', 'T40.3X3D', 'T40.3X3S', 'T40.3X4', 'T40.3X4A', 'T40.3X4D', 'T40.3X4S', 'T40.3X6', 'T40.3X6A', 'T40.3X6D', 'T40.3X6S', 'T40.4', 'T40.4X', 'T40.4X1', 'T40.4X1A', 'T40.4X1D', 'T40.4X1S', 'T40.4X2', 'T40.4X2A', 'T40.4X2D', 'T40.4X2S', 'T40.4X3', 'T40.4X3A', 'T40.4X3D', 'T40.4X3S', 'T40.4X4', 'T40.4X4A', 'T40.4X4D', 'T40.4X4S', '965.01', 'E85.00', '965.00', '965.02', '965.09', 'E85.01', 'E85.02')
        )
-- limit 100000
)
select --get ptnt data for maternity keywords that also contain drug keywords 
    -- c2.RSN_CD1_CDNG_T ,
    -- count(*) c,
    *
from cte2 c2
where (CONTAINS(upper(RSN_CD1_CDNG_T),     'PREG')
		OR CONTAINS(upper(RSN_CD1_CDNG_T), 'PRENATAL')
		OR CONTAINS(upper(RSN_CD1_CDNG_T), 'PREGNANCY')
		OR CONTAINS(upper(RSN_CD1_CDNG_T), 'PREG OVERDOSE')
        OR CONTAINS(upper(RSN_CD1_CDNG_T), 'PREG & OVERDOSE')
        OR CONTAINS(upper(RSN_CD1_CDNG_T), 'MATERNAL' )
        )
;



---scenario 2 
/*
Get patient data that contains drug keywords OR maternity cases. This query results in a higher count since the query is says maternity keywords or drug keywords 
*/
--CONTAINS
with cte as (
SELECT --get base ptnt ER data for TN
    e.RSN_CD1_CDNG_T, --some times this and the cd_cdng_c will be different what the dr says
    c.cd_cdng_c, --the actual doctor inputed code
    c.cd_cdng_t, --try this to see what the txt looks like here in relation to cd_cdng_c
    e.ptnt_ref,
    e.PERIOD_START,
    p.address_state,
    e.PERIOD_END, 
    e.ENCOUNTER_ID,
    e.ECDH_SUBMITTED_FILE_TYPE,
    E.ECDH_SUBMITTER_NAME
   from ECT_PRD_ECDH_DB.FOUNDATION.ENCOUNTER e
        inner join ECT_PRD_ECDH_DB.FOUNDATION.PATIENT p 
            on e.ptnt_ref = p.ptnt_id
        INNER JOIN ECT_PRD_ECDH_DB.FOUNDATION.condition AS C 
            ON E.encounter_id = c.encounter_ref
        --add org table with name
        --refer to table join criteria 
where   (
        UPPER(e.ECDH_SUBMITTER_NAME) in ('CDXTHATC', 'EPP','HOSPITAL-CORP-OF-AMERICA','WTHC')  
        --OR p.ADDRESS_STATE ='TN'--potentially remove TN --test removing this
        ) 
    -- AND P.ECDH_CS_LOB_FLAG = 'Y' --test to remove 
    and e.ECDH_SUBMITTED_FILE_TYPE ='ADT'
    -- and e.ENCOUNTER_CLASS_CDNG_C IN ('ERU', 'ER', 'EMERGENCY', 'EMER') 
    --limit 10000
    ),
cte2 as ( --get ptnt keyword data for maternity cases
    select * 
    from cte as c1
        where (contains(UPPER(rsn_cd1_cdng_t),'ALCOHOL')
        OR contains(UPPER(rsn_cd1_cdng_t),'AMPHETAMINES')
        OR contains(UPPER(rsn_cd1_cdng_t),'AVINZA')
        OR contains(UPPER(rsn_cd1_cdng_t),'BARBITURATES')
        OR contains(UPPER(rsn_cd1_cdng_t),'BENZO ')
        OR contains(UPPER(rsn_cd1_cdng_t),'BENZODIAZEPINE')
        OR contains(UPPER(rsn_cd1_cdng_t),'BLOOD ALCOHOL')
        OR contains(UPPER(rsn_cd1_cdng_t),'BUPRENORPHINE')
        OR contains(UPPER(rsn_cd1_cdng_t),'CANNABI')
        OR contains(UPPER(rsn_cd1_cdng_t),'COCAINE')
        OR contains(UPPER(rsn_cd1_cdng_t),'CODEINE')
        OR contains(UPPER(rsn_cd1_cdng_t),'CRACK')
        OR contains(UPPER(rsn_cd1_cdng_t),'DETOX')
        OR contains(UPPER(rsn_cd1_cdng_t),'DILAUDID')
        OR contains(UPPER(rsn_cd1_cdng_t),'DRUG ABUSE')
        OR contains(UPPER(rsn_cd1_cdng_t),'DRUG DEPENDENCE')
        OR contains(UPPER(rsn_cd1_cdng_t),'DURAGESIC')
        OR contains(UPPER(rsn_cd1_cdng_t),'ETHANOL')
        OR contains(UPPER(rsn_cd1_cdng_t),'FENTANYL')
        OR contains(UPPER(rsn_cd1_cdng_t),'HALLUCINOGEN')
        OR contains(UPPER(rsn_cd1_cdng_t),'HEROIN')
        OR contains(UPPER(rsn_cd1_cdng_t),'HYDROCODONE')
        OR contains(UPPER(rsn_cd1_cdng_t),'HYPNOTIC')
        OR contains(UPPER(rsn_cd1_cdng_t),'INTOXICATION')
        OR contains(UPPER(rsn_cd1_cdng_t),'IVDU')
        OR contains(UPPER(rsn_cd1_cdng_t),'LSD')
        OR contains(UPPER(rsn_cd1_cdng_t),'METHADONE')
        OR contains(UPPER(rsn_cd1_cdng_t),'METHAMPHETAMINE')
        OR contains(UPPER(rsn_cd1_cdng_t),'METHYLPHENIDATE')
        OR contains(UPPER(rsn_cd1_cdng_t),'MORPHINE')
        OR contains(UPPER(rsn_cd1_cdng_t),'NALOXONE')
        OR contains(UPPER(rsn_cd1_cdng_t),'NALTREXONE')
        OR contains(UPPER(rsn_cd1_cdng_t),'NARCAN')
        OR contains(UPPER(rsn_cd1_cdng_t),'NARCOTIC')
        OR contains(UPPER(rsn_cd1_cdng_t),'O.D.')
        -- OR contains(UPPER(rsn_cd1_cdng_t),'OD')
        OR contains(UPPER(rsn_cd1_cdng_t),'OPANA')
        OR contains(UPPER(rsn_cd1_cdng_t),'OPIATE')
        OR contains(UPPER(rsn_cd1_cdng_t),'OPIOID')
        OR contains(UPPER(rsn_cd1_cdng_t),'OPIUM')
        OR contains(UPPER(rsn_cd1_cdng_t),'OUD)')
        OR contains(UPPER(rsn_cd1_cdng_t),'OVERDOSE')
        OR contains(UPPER(rsn_cd1_cdng_t),'OXYCODONE')
        OR contains(UPPER(rsn_cd1_cdng_t),'OXYCONTIN')
        OR contains(UPPER(rsn_cd1_cdng_t),'PERCOCET')
        OR contains(UPPER(rsn_cd1_cdng_t),'POLYDRUG')
        OR contains(UPPER(rsn_cd1_cdng_t),'POLYSUB')
        OR contains(UPPER(rsn_cd1_cdng_t),'PSUD')
        OR contains(UPPER(rsn_cd1_cdng_t),'PSYCHOACTIVE')
        OR contains(UPPER(rsn_cd1_cdng_t),'SEDATIVE')
        OR contains(UPPER(rsn_cd1_cdng_t),'STIMULANT')
        OR contains(UPPER(rsn_cd1_cdng_t),'SUBOXONE')
        OR contains(UPPER(rsn_cd1_cdng_t),'VICODIN')
        )
        or (c1.cd_cdng_c IN ('T40','F11', 'P96.1', 'T40.6', 'T40.60', 'T40.601', 'T40.601A', 'T40.601D', 'T40.601S', 'T40.602', 'T40.602A', 'T40.602D', 'T40.602S', 'T40.603', 'T40.603A', 'T40.603D', 'T40.603S', 'T40.604', 'T40.604A', 'T40.604D', 'T40.604S', 'T40.69', 'T40.691', 'T40.691A', 'T40.691D', 'T40.691S', 'T40.692', 'T40.692A', 'T40.692D', 'T40.692S', 'T40.693', 'T40.693A', 'T40.693D', 'T40.693S', 'T40.694', 'T40.694A', 'T40.694D', 'T40.694S', 'T40.', 'T40.0', 'T40.0X', 'T40.0X1', 'T40.0X1A', 'T40.0X1D', 'T40.0X1S', 'T40.0X2', 'T40.0X2A', 'T40.0X2D', 'T40.0X2S', 'T40.0X3', 'T40.0X3A', 'T40.0X3D', 'T40.0X3S', 'T40.0X4', 'T40.0X4A', 'T40.0X4D', 'T40.0X4S', 'T40.0X6', 'T40.0X6A', 'T40.0X6D', 'T40.0X6S', 'T40.1X1A', 'T40.1X1D', 'T40.1X4A', 'T40.1X4D', 'T40.1X4S', 'T40.2', 'T40.2X', 'T40.2X1', 'T40.2X1A', 'T40.2X1D', 'T40.2X1S', 'T40.2X2', 'T40.2X2A', 'T40.2X2D', 'T40.2X2S', 'T40.2X3', 'T40.2X3A', 'T40.2X3D', 'T40.2X3S', 'T40.2X4', 'T40.2X4A', 'T40.2X4D', 'T40.2X4S', 'T40.2X6', 'T40.2X6A', 'T40.2X6D', 'T40.2X6S', 'T40.3', 'T40.3X', 'T40.3X1', 'T40.3X1A', 'T40.3X1D', 'T40.3X1S', 'T40.3X2', 'T40.3X2A', 'T40.3X2D', 'T40.3X2S', 'T40.3X3', 'T40.3X3A', 'T40.3X3D', 'T40.3X3S', 'T40.3X4', 'T40.3X4A', 'T40.3X4D', 'T40.3X4S', 'T40.3X6', 'T40.3X6A', 'T40.3X6D', 'T40.3X6S', 'T40.4', 'T40.4X', 'T40.4X1', 'T40.4X1A', 'T40.4X1D', 'T40.4X1S', 'T40.4X2', 'T40.4X2A', 'T40.4X2D', 'T40.4X2S', 'T40.4X3', 'T40.4X3A', 'T40.4X3D', 'T40.4X3S', 'T40.4X4', 'T40.4X4A', 'T40.4X4D', 'T40.4X4S', '965.01', 'E85.00', '965.00', '965.02', '965.09', 'E85.01', 'E85.02')
        )
or (CONTAINS(upper(RSN_CD1_CDNG_T),   'PREG')
		OR CONTAINS(upper(RSN_CD1_CDNG_T), 'PRENATAL')
		OR CONTAINS(upper(RSN_CD1_CDNG_T), 'PREGNANCY')
		OR CONTAINS(upper(RSN_CD1_CDNG_T), 'PREG OVERDOSE')
        OR CONTAINS(upper(RSN_CD1_CDNG_T), 'PREG & OVERDOSE')
        OR CONTAINS(upper(RSN_CD1_CDNG_T), 'MATERNAL' )
    )
)
select 
    -- c2.RSN_CD1_CDNG_T ,
    -- count(*) c,
    *
from cte2 c2
;


-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- refined solution

WITH cte AS (
    SELECT 
        e.RSN_CD1_CDNG_T, -- reason code text
        c.cd_cdng_c, -- doctor inputted code
        c.cd_cdng_t, -- code description
        e.ptnt_ref,
        e.PERIOD_START,
        p.address_state,
        e.PERIOD_END, 
        e.ENCOUNTER_ID,
        e.ECDH_SUBMITTED_FILE_TYPE,
        e.ECDH_SUBMITTER_NAME
    FROM ECT_PRD_ECDH_DB.FOUNDATION.ENCOUNTER e
    INNER JOIN ECT_PRD_ECDH_DB.FOUNDATION.PATIENT p 
        ON e.ptnt_ref = p.ptnt_id
    INNER JOIN ECT_PRD_ECDH_DB.FOUNDATION.condition c 
        ON e.encounter_id = c.encounter_ref
    WHERE 
        UPPER(e.ECDH_SUBMITTER_NAME) IN ('CDXTHATC', 'EPP', 'HOSPITAL-CORP-OF-AMERICA', 'WTHC')
        AND e.ECDH_SUBMITTED_FILE_TYPE = 'ADT'
),
cte2 AS ( 
    -- Filter patients based on drug-related keywords or specific codes
    SELECT * 
    FROM cte AS c1
    WHERE (
        -- Exact keyword match for drugs
        UPPER(rsn_cd1_cdng_t) LIKE '%ALCOHOL%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%AMPHETAMINES%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%AVINZA%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%BARBITURATES%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%BENZO%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%BENZODIAZEPINE%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%BLOOD ALCOHOL%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%BUPRENORPHINE%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%CANNABI%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%COCAINE%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%CODEINE%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%CRACK%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%DETOX%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%DILAUDID%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%DRUG ABUSE%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%DRUG DEPENDENCE%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%DURAGESIC%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%ETHANOL%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%FENTANYL%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%HALLUCINOGEN%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%HEROIN%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%HYDROCODONE%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%HYPNOTIC%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%INTOXICATION%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%IVDU%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%LSD%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%METHADONE%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%METHAMPHETAMINE%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%METHYLPHENIDATE%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%MORPHINE%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%NALOXONE%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%NALTREXONE%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%NARCAN%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%NARCOTIC%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%O.D.%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%OPANA%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%OPIATE%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%OPIOID%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%OPIUM%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%OUD%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%OVERDOSE%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%OXYCODONE%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%OXYCONTIN%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%PERCOCET%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%POLYDRUG%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%POLYSUB%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%PSUD%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%PSYCHOACTIVE%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%SEDATIVE%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%STIMULANT%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%SUBOXONE%' OR
        UPPER(rsn_cd1_cdng_t) LIKE '%VICODIN%'
    )
    OR c1.cd_cdng_c IN (
        'T40','F11', 'P96.1', 'T40.6', 'T40.60', 'T40.601', 'T40.601A', 'T40.601D', 'T40.601S',
        -- Continue code matching as before...
    )
)

-- Final select for pregnancy-related keywords
SELECT *
FROM cte2 c2
WHERE (
    UPPER(RSN_CD1_CDNG_T) LIKE '%PREG%' OR
    UPPER(RSN_CD1_CDNG_T) LIKE '%PRENATAL%' OR
    UPPER(RSN_CD1_CDNG_T) LIKE '%PREGNANCY%' OR
    UPPER(RSN_CD1_CDNG_T) LIKE '%PREG OVERDOSE%' OR
    UPPER(RSN_CD1_CDNG_T) LIKE '%PREG & OVERDOSE%' OR
    UPPER(RSN_CD1_CDNG_T) LIKE '%MATERNAL%'
);


-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

WITH cte AS (
    SELECT 
        e.RSN_CD1_CDNG_T, -- reason code text
        c.cd_cdng_c, -- doctor inputted code
        c.cd_cdng_t, -- code description
        e.ptnt_ref,
        e.PERIOD_START,
        p.address_state,
        e.PERIOD_END, 
        e.ENCOUNTER_ID,
        e.ECDH_SUBMITTED_FILE_TYPE,
        e.ECDH_SUBMITTER_NAME
    FROM ECT_PRD_ECDH_DB.FOUNDATION.ENCOUNTER e
    INNER JOIN ECT_PRD_ECDH_DB.FOUNDATION.PATIENT p 
        ON e.ptnt_ref = p.ptnt_id
    INNER JOIN ECT_PRD_ECDH_DB.FOUNDATION.CONDITION c 
        ON e.encounter_id = c.encounter_ref
    WHERE 
        UPPER(e.ECDH_SUBMITTER_NAME) IN ('CDXTHATC', 'EPP', 'HOSPITAL-CORP-OF-AMERICA', 'WTHC')
        AND e.ECDH_SUBMITTED_FILE_TYPE = 'ADT'
),
cte2 AS (
    -- Filter patients based on drug-related keywords using REGEXP_INSTR for flexibility
    SELECT * 
    FROM cte AS c1
    WHERE (
        REGEXP_INSTR(UPPER(c1.RSN_CD1_CDNG_T), 'ALCOHOL|AMPHETAMINES|AVINZA|BARBITURATES|BENZO|BENZODIAZEPINE|BLOOD ALCOHOL|BUPRENORPHINE|CANNABI|COCAINE|CODEINE|CRACK|DETOX|DILAUDID|DRUG ABUSE|DRUG DEPENDENCE|DURAGESIC|ETHANOL|FENTANYL|HALLUCINOGEN|HEROIN|HYDROCODONE|HYPNOTIC|INTOXICATION|IVDU|LSD|METHADONE|METHAMPHETAMINE|METHYLPHENIDATE|MORPHINE|NALOXONE|NALTREXONE|NARCAN|NARCOTIC|O\.D\.|OPANA|OPIATE|OPIOID|OPIUM|OUD|OVERDOSE|OXYCODONE|OXYCONTIN|PERCOCET|POLYDRUG|POLYSUB|PSUD|PSYCHOACTIVE|SEDATIVE|STIMULANT|SUBOXONE|VICODIN') > 0
        OR c1.cd_cdng_c IN (
            'T40','F11', 'P96.1', 'T40.6', 'T40.60', 'T40.601', 'T40.601A', 'T40.601D', 'T40.601S',
            'T40.602', 'T40.602A', 'T40.602D', 'T40.602S', 'T40.603', 'T40.603A', 'T40.603D', 'T40.603S', 
            'T40.604', 'T40.604A', 'T40.604D', 'T40.604S', 'T40.69', 'T40.691', 'T40.691A', 'T40.691D', 'T40.691S',
            -- Add additional codes here as needed
            '965.01', '965.00', '965.02', '965.09', 'E85.00', 'E85.01', 'E85.02'
        )
    )
),
cte3 AS (
    -- Filter maternity-related keywords
    SELECT * 
    FROM cte2 c2
    WHERE REGEXP_INSTR(UPPER(c2.RSN_CD1_CDNG_T), 'PREG|PRENATAL|PREGNANCY|PREG OVERDOSE|MATERNAL') > 0
)

-- Final select
SELECT *
FROM cte3;

-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
WITH cte AS (
    -- Get the base patient encounter data
    SELECT 
        e.RSN_CD1_CDNG_T,  -- reason code text
        c.cd_cdng_c,       -- condition code (doctor input)
        e.ENCOUNTER_ID
    FROM ECT_PRD_ECDH_DB.FOUNDATION.ENCOUNTER e
    INNER JOIN ECT_PRD_ECDH_DB.FOUNDATION.CONDITION c 
        ON e.encounter_id = c.encounter_ref
    WHERE UPPER(e.ECDH_SUBMITTED_FILE_TYPE) = 'ADT'
),

keyword_match AS (
    -- Records matching the keyword list in the reason code text
    SELECT DISTINCT
        ENCOUNTER_ID
    FROM cte
    WHERE REGEXP_INSTR(
        UPPER(RSN_CD1_CDNG_T), 
        'ALCOHOL|AMPHETAMINES|AVINZA|BARBITURATES|BENZO|BENZODIAZEPINE|BLOOD ALCOHOL|BUPRENORPHINE|CANNABI|COCAINE|CODEINE|CRACK|DETOX|DILAUDID|DRUG ABUSE|DRUG DEPENDENCE|DURAGESIC|ETHANOL|FENTANYL|HALLUCINOGEN|HEROIN|HYDROCODONE|HYPNOTIC|INTOXICATION|IVDU|LSD|METHADONE|METHAMPHETAMINE|METHYLPHENIDATE|MORPHINE|NALOXONE|NALTREXONE|NARCAN|NARCOTIC|O\.D\.|OPANA|OPIATE|OPIOID|OPIUM|OUD|OVERDOSE|OXYCODONE|OXYCONTIN|PERCOCET|POLYDRUG|POLYSUB|PSUD|PSYCHOACTIVE|SEDATIVE|STIMULANT|SUBOXONE|VICODIN'
    ) > 0
),

condition_code_match AS (
    -- Records matching the condition code list
    SELECT DISTINCT
        ENCOUNTER_ID
    FROM cte
    WHERE cd_cdng_c IN (
        'T40','F11', 'P96.1', 'T40.6', 'T40.60', 'T40.601', 'T40.601A', 'T40.601D', 'T40.601S',
        'T40.602', 'T40.602A', 'T40.602D', 'T40.602S', 'T40.603', 'T40.603A', 'T40.603D', 'T40.603S',
        '965.01', '965.00', '965.02', '965.09', 'E85.00', 'E85.01', 'E85.02'
        -- Add more condition codes as necessary
    )
),

both_match AS (
    -- Records that match both the keyword list and the condition code list
    SELECT 
        DISTINCT k.ENCOUNTER_ID
    FROM keyword_match k
    JOIN condition_code_match c
        ON k.ENCOUNTER_ID = c.ENCOUNTER_ID
)

-- Final output with counts
SELECT
    -- Total records that match the keyword list
    (SELECT COUNT(*) FROM keyword_match) AS keyword_match_count,
    
    -- Total records that match the condition code list
    (SELECT COUNT(*) FROM condition_code_match) AS condition_code_match_count,
    
    -- Total unique records that match both the keyword list and condition code list
    (SELECT COUNT(*) FROM both_match) AS both_match_count;


-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
WITH cte AS (
    -- Get the base patient encounter data
    SELECT 
        e.RSN_CD1_CDNG_T,  -- reason code text
        c.cd_cdng_c,       -- condition code (doctor input)
        e.ENCOUNTER_ID
    FROM ECT_PRD_ECDH_DB.FOUNDATION.ENCOUNTER e
    INNER JOIN ECT_PRD_ECDH_DB.FOUNDATION.CONDITION c 
        ON e.encounter_id = c.encounter_ref
    WHERE UPPER(e.ECDH_SUBMITTED_FILE_TYPE) = 'ADT'
),

cte_with_flags AS (
    -- Use CASE statements to flag records that match keywords and condition codes
    SELECT
        ENCOUNTER_ID,
        -- Flag for keyword match
        CASE 
            WHEN REGEXP_INSTR(
                UPPER(RSN_CD1_CDNG_T), 
                'ALCOHOL|AMPHETAMINES|AVINZA|BARBITURATES|BENZO|BENZODIAZEPINE|BLOOD ALCOHOL|BUPRENORPHINE|CANNABI|COCAINE|CODEINE|CRACK|DETOX|DILAUDID|DRUG ABUSE|DRUG DEPENDENCE|DURAGESIC|ETHANOL|FENTANYL|HALLUCINOGEN|HEROIN|HYDROCODONE|HYPNOTIC|INTOXICATION|IVDU|LSD|METHADONE|METHAMPHETAMINE|METHYLPHENIDATE|MORPHINE|NALOXONE|NALTREXONE|NARCAN|NARCOTIC|O\.D\.|OPANA|OPIATE|OPIOID|OPIUM|OUD|OVERDOSE|OXYCODONE|OXYCONTIN|PERCOCET|POLYDRUG|POLYSUB|PSUD|PSYCHOACTIVE|SEDATIVE|STIMULANT|SUBOXONE|VICODIN'
            ) > 0
            THEN 1
            ELSE 0
        END AS keyword_match_flag,

        -- Flag for condition code match
        CASE 
            WHEN cd_cdng_c IN (
                'T40','F11', 'P96.1', 'T40.6', 'T40.60', 'T40.601', 'T40.601A', 'T40.601D', 'T40.601S',
                'T40.602', 'T40.602A', 'T40.602D', 'T40.602S', 'T40.603', 'T40.603A', 'T40.603D', 'T40.603S',
                '965.01', '965.00', '965.02', '965.09', 'E85.00', 'E85.01', 'E85.02'
            )
            THEN 1
            ELSE 0
        END AS condition_code_match_flag
    FROM cte
)

-- Final output with counts
SELECT
    -- Total records that match the keyword list
    COUNT(DISTINCT CASE WHEN keyword_match_flag = 1 THEN ENCOUNTER_ID END) AS keyword_match_count,
    
    -- Total records that match the condition code list
    COUNT(DISTINCT CASE WHEN condition_code_match_flag = 1 THEN ENCOUNTER_ID END) AS condition_code_match_count,
    
    -- Total unique records that match both the keyword list and the condition code list
    COUNT(DISTINCT CASE WHEN keyword_match_flag = 1 AND condition_code_match_flag = 1 THEN ENCOUNTER_ID END) AS both_match_count
FROM cte_with_flags;

