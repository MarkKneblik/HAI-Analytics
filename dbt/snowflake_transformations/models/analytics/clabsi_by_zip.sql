-- models/analytics/clabsi_by_zip.sql

{{ config(schema = 'ANALYTICS') }}

-- Filter for CLABSI SIR records only
WITH all_clabsi_records AS (
    SELECT *
    FROM {{ ref('int_hai_data_enriched') }}
    WHERE MEASURE_ID = 'HAI_1_SIR'
)

-- Aggregate average CLABSI SIR by ZIP code
SELECT 
    ZIP_CODE, 
    AVG(SCORE) AS avg_sir
FROM all_clabsi_records
GROUP BY ZIP_CODE