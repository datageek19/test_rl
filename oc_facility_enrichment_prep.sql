-- facility enrichment

select
  KVALUE:rawPayload::string,
  substring(tracking_id,1,4) as submitter,
  split_part(msh21_msh3.value,'^',2) as msh3_code,
  split_part(msh21_msh3.value,'^',3) as msh3_oid,
  split_part(msh21_msh4.value,'^',2) as msh4_code,
  split_part(msh21_msh4.value,'^',3) as msh4_oid,
  split_part(split_part(enc.value,'|',4),'^',1) as pv1_3_1,
  split_part(split_part(enc.value,'|',4),'^',2) as pv1_3_2,
  split_part(split_part(enc.value,'|',4),'^',3) as pv1_3_3,
  split_part(split_part(split_part(enc.value,'|',4),'^',4),'&',1) as pv1_3_4_1,
  split_part(split_part(split_part(enc.value,'|',4),'^',4),'&',2) as pv1_3_4_2,
  split_part(enc.value,'|',40) as pv1_39
from OCDM_RAW_FILES.OCG_MSFHIR_ADT SRC 
  join lateral flatten(split(KVALUE:rawPayload,'\r')) pid
  join lateral flatten(split(KVALUE:rawPayload,'\r')) enc
  --join lateral flatten(input => split(KVALUE:rawPayload,'\r')) pv2
  join lateral flatten(split(KVALUE:rawPayload,'\r')) msh
  join lateral flatten (split(split_part(msh.value,'|',21),'~')) msh21_msh3
  join lateral flatten (split(split_part(msh.value,'|',21),'~')) msh21_msh4
  where 
  msh.index = 0 -- this is MSH
  and split_part(pid.value::string,'|',1) = 'PID'
  and split_part(enc.value::string,'|',1) = 'PV1'
  --and split_part(pv2.value::string,'|',1) = 'PV2'
  and substring(msh21_msh3.value::string,1,6) = '@MSH.3'
  and substring(msh21_msh4.value::string,1,6) = '@MSH.4'