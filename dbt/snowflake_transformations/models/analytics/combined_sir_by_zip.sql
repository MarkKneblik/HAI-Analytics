-- models/analytics/combined_sir_by_zip.sql

{{ config(schema = 'ANALYTICS') }}

-- Pull CLABSI SIR records by ZIP
WITH all_clabsi_records AS (
    SELECT 
        ZIP_CODE,
        AVG_SIR AS CLABSI_SIR
    FROM {{ ref('clabsi_by_zip') }}
),

-- Pull CAUTI SIR records by ZIP
all_cauti_records AS (
    SELECT 
        ZIP_CODE,
        AVG_SIR AS CAUTI_SIR
    FROM {{ ref('cauti_by_zip') }}
)

/*
    Combine CLABSI and CAUTI AVG_SIRs by ZIP
    FULL OUTER JOIN to ensure all ZIPs are included,
    whether they exist in one or both views
    COALESCE handles NULLs by replacing them with 0
*/

SELECT 
    COALESCE(all_clabsi_records.ZIP_CODE, all_cauti_records.ZIP_CODE) AS ZIP_CODE,
    COALESCE(all_clabsi_records.CLABSI_SIR, 0) AS CLABSI_SIR,
    COALESCE(all_cauti_records.CAUTI_SIR, 0) AS CAUTI_SIR,
    COALESCE(all_clabsi_records.CLABSI_SIR, 0) + COALESCE(all_cauti_records.CAUTI_SIR, 0) AS COMBINED_SIR
FROM 
    all_clabsi_records
FULL OUTER JOIN 
    all_cauti_records
ON 
    all_clabsi_records.ZIP_CODE = all_cauti_records.ZIP_CODE