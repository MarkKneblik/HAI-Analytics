-- models/analytics/cauti_by_zip.sql

{{ config(schema = 'ANALYTICS') }}

-- Filter for CAUTI SIR records only
WITH all_cauti_records AS (
    SELECT *
    FROM {{ ref('int_hai_data_enriched') }}
    WHERE MEASURE_ID = 'HAI_2_SIR'
)

-- Aggregate average CAUTI SIR by ZIP code
SELECT 
    ZIP_CODE, 
    AVG(SCORE) AS avg_sir
FROM all_cauti_records
GROUP BY ZIP_CODE