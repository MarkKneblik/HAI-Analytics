-- models/intermediate/int_hai_data_enriched.sql

{{ config(schema = 'INTERMEDIATE') }}

-- Create categories for infections for easier aggregation
SELECT 
    *,
    CASE
        WHEN MEASURE_ID LIKE 'HAI_1%' THEN 'CLABSI'
        WHEN MEASURE_ID LIKE 'HAI_2%' THEN 'CAUTI'
        ELSE 'OTHER'  -- Catch any unexpected or new measure IDs
    END AS INFECTION_TYPE
FROM {{ ref('int_hai_data_filtered') }}