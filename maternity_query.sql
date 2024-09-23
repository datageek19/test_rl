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
