prevalent analysis code:

WITH total_per_submitter AS (
    SELECT submitter, COUNT(*) AS total_count
    FROM your_table
    GROUP BY submitter
),
prevalence_count AS (
    SELECT submitter, facility_name, COUNT(*) AS prevalent_count
    FROM your_table
    GROUP BY submitter, facility_name
)
SELECT 
    pc.submitter, 
    pc.facility_name, 
    pc.prevalent_count, 
    (pc.prevalent_count::float / tps.total_count::float) * 100 AS prevalent_percentage
FROM prevalence_count pc
JOIN total_per_submitter tps ON pc.submitter = tps.submitter
ORDER BY pc.submitter, pc.facility_name;
