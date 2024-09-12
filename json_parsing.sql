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
from source_raw_file_tables SRC 
  join lateral flatten(split(KVALUE:rawPayload,'\r')) pid
  join lateral flatten(split(KVALUE:rawPayload,'\r')) enc
  --join lateral flatten(input => split(KVALUE:rawPayload,'\r')) pv2
  join lateral flatten(split(KVALUE:rawPayload,'\r')) msh
  join lateral flatten (split(split_part(msh.value,'|',21),'~')) msh21_msh3
  join lateral flatten (split(split_part(msh.value,'|',21),'~')) msh21_msh4
  where 
  submitter='9060'
  and msh.index = 0 -- this is MSH
  and split_part(pid.value::string,'|',1) = 'PID'
  and split_part(enc.value::string,'|',1) = 'PV1'
  -- and split_part(pv2.value::string,'|',1) = 'PV2'
  and substring(msh21_msh3.value::string,1,6) = '@MSH.3'
  and substring(msh21_msh4.value::string,1,6) = '@MSH.4'

++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- updated solution

WITH raw_data AS (
  SELECT
    KVALUE:rawPayload::string AS raw_payload,
    substring(tracking_id, 1, 4) AS submitter
  FROM source_raw_file_tables SRC
),
pid_data AS (
  SELECT
    raw_payload,
    split_part(pid.value::string, '|', 1) AS pid_segment,
    pid.value
  FROM raw_data
  JOIN LATERAL flatten(split(raw_payload, '\r')) pid
  WHERE split_part(pid.value::string, '|', 1) = 'PID'
),
enc_data AS (
  SELECT
    raw_payload,
    enc.value
  FROM raw_data
  JOIN LATERAL flatten(split(raw_payload, '\r')) enc
  WHERE split_part(enc.value::string, '|', 1) = 'PV1'
),
msh_data AS (
  SELECT
    raw_payload,
    split_part(msh.value, '|', 21) AS msh21_value,
    msh.value
  FROM raw_data
  JOIN LATERAL flatten(split(raw_payload, '\r')) msh
  WHERE msh.index = 0 -- This is MSH
),
msh_split AS (
  SELECT
    raw_payload,
    split_part(msh21_value, '~', 2) AS msh3_code,
    split_part(msh21_value, '~', 3) AS msh3_oid,
    split_part(msh21_value, '~', 2) AS msh4_code,
    split_part(msh21_value, '~', 3) AS msh4_oid
  FROM msh_data
  JOIN LATERAL flatten(split(msh21_value, '~')) msh_split
  WHERE substring(msh_split.value::string, 1, 6) IN ('@MSH.3', '@MSH.4')
)
SELECT
  raw_payload,
  pid_data.value AS pid_value,
  enc_data.value AS enc_value,
  msh_split.msh3_code,
  msh_split.msh3_oid,
  msh_split.msh4_code,
  msh_split.msh4_oid,
  split_part(split_part(enc_data.value, '|', 4), '^', 1) AS pv1_3_1,
  split_part(split_part(enc_data.value, '|', 4), '^', 2) AS pv1_3_2,
  split_part(split_part(enc_data.value, '|', 4), '^', 3) AS pv1_3_3,
  split_part(split_part(split_part(enc_data.value, '|', 4), '^', 4), '&', 1) AS pv1_3_4_1,
  split_part(split_part(split_part(enc_data.value, '|', 4), '^', 4), '&', 2) AS pv1_3_4_2,
  split_part(enc_data.value, '|', 40) AS pv1_39
FROM pid_data
JOIN enc_data ON pid_data.raw_payload = enc_data.raw_payload
JOIN msh_split ON pid_data.raw_payload = msh_split.raw_payload
WHERE pid_data.submitter = '9060';
