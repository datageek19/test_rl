-- preenrichment table count
SELECT 
	admitsourcepv114, admitsourcepv141, ptclasspv121, msgttypemsh91, COUNT(*) 
FROM iris_adt_ds.cdsm_memberdemo_adtpreenrichmenttable_parquet
where ptadmitdate >= '2023-03-01' and msgttypemsh91 = 'ADT'
GROUP by admitsourcepv114 , admitsourcepv141 , ptclasspv121, msgttypemsh91 ;

-- postenrichment table count
SELECT 
	admitsourcepv114, admitsourcepv141, ptclasspv121, msgttypemsh91, COUNT(*) 
FROM iris_adt_ds.cdsm_memberdemo_adtpostenrichmenttable_parquet 
where ptadmitdate >= '2023-03-01' and msgttypemsh91 = 'ADT'
GROUP by admitsourcepv114 , admitsourcepv141 , ptclasspv121, msgttypemsh91 ;

-- ------------------------------------------------------------------------------------
SELECT
    COUNT(
        CASE
            WHEN 
                pre.admitsourcepv141 ='Emergency Accident Thru ER' 
                AND pre.admitsourcepv114  = 'Emergency' 
                AND post.admitsourcepv141 =1
                AND post.admitsourcepv114 = 'Emergency'
            THEN 1 ELSE 0

            WHEN
                pre.admitsourcepv141 = 'TL' 
                AND pre.admitsourcepv141='Multi Service' 
                AND post.admitsourcepv141 = 'logic'
            THEN
                -- handle logic in this case statement
                CASE
                    WHEN pre.ptclasspv121 = 'O' and post.admitsourcepv141='C' and post.admitsourcepv114='Elective' THEN 1 ELSE 0
                    WHEN pre.ptclasspv121 = 'E' and post.admitsourcepv141='E' and post.admitsourcepv114='Emergency' THEN 1 ELSE 0
                END
        END
    )
FROM iris_adt_ds.cdsm_memberdemo_adtpreenrichmenttable_parquet as pre
outer join iris_adt_ds.cdsm_memberdemo_adtpostenrichmenttable_parquet as post
on pre.tracking_id= post.tracking_id 
    and pre.admitprovfnamepv1173=post.admitprovfnamepv1173 
    and pre.admitprovlnamepv1172=post.admitprovlnamepv1172
    and pre.admitprovmiddlenamepv1174  =post.admitprovmiddlenamepv1174
where ptadmitdate >= '2023-03-01' and msgttypemsh91 = 'ADT'



--
-- SELECT
--     COUNT(
--     	CASE 
--         	WHEN LOWER(admitsourcepv141) ='Emergency Accident Thru ER' AND LOWER(admitsourcepv114)  = 'Emergency' THEN 1 ELSE 0,
--         	WHEN LOWER(admitsourcepv141) = 'TL' AND LOWER(admitsourcepv114) = 'Multi Services'
--         END
--     	) AS var1,
-- FROM iris_adt_ds.cdsm_memberdemo_adtpostenrichmenttable_parquet
-- where ptadmitdate >= '2023-03-01' and msgttypemsh91 = 'ADT'

