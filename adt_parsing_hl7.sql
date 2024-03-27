-- test for oc adt parsing

SELECT 
  rawPayload::string,
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
FROM 
  (SELECT KVALUE:rawPayload FROM OCDM_RAW_FILES.OCG_MSFHIR_ADT) SRC
  LATERAL FLATTEN(input => split(rawPayload, '\r')) segments
  LATERAL FLATTEN(input => split(segments.value, '|')) fields
  LATERAL FLATTEN(input => split(fields.value, '^')) components
  LATERAL FLATTEN(input => split(components.value, '&')) subcomponents
WHERE 
  split_part(segments.value::string,'|',1) = 'MSH'
  AND split_part(fields.value::string,'|',1) = 'PID'
  AND split_part(components.value::string,'|',1) = 'PV1'
  AND substring(subcomponents.value::string,1,6) = '@MSH.3'
  AND substring(subcomponents.value::string,1,6) = '@MSH.4'
