-- models/intermediate/int_hai_data_filtered.sql

{{ config(schema = 'INTERMEDIATE') }}

/* 
    Filter for HAI_1_ELIGCASES, HAI_1_NUMERATOR, HAI_1_SIR, HAI_2_ELIGCASES, HAI_2_NUMERATOR, 
    and HAI_2_SIR (desired measures associated with CLABSI and CAUTI infections)
*/

SELECT * 
FROM {{ source('HAI_DATABASE', 'HAI_DATA') }}
WHERE (MEASURE_ID LIKE 'HAI_1%' OR MEASURE_ID LIKE 'HAI_2%')
  AND MEASURE_ID NOT IN ('HAI_1_CILOWER', 'HAI_1_CIUPPER', 'HAI_1_DOPC', 'HAI_2_CILOWER', 'HAI_2_CIUPPER', 'HAI_2_DOPC')
  -- Exclude rows where COMPARED_TO_NATIONAL is NULL or 'Not Available' (invalid values)
  AND COMPARED_TO_NATIONAL IS NOT NULL
  AND COMPARED_TO_NATIONAL != 'Not Available'