-- models/analytics/clabsi_by_state.sql

{{ config(schema = 'ANALYTICS') }}

/*
    For each state, this model calculates the total number of unique healthcare facilities reporting
    the Standardized Infection Ratio (SIR) for CLABSI (Central Line-Associated Bloodstream Infection),
    counts how many facilities perform worse than the national benchmark, 
    and computes the percentage of worse-performing facilities per state.

    This model helps identify states with higher proportions of facilities needing
    infection control improvement.

    Note: States with no facilities worse than the national benchmark will show NULL worse_count.
*/

-- Filter the dataset to only include CLABSI records with Standardized Infection Ratio (SIR) metric (MEASURE_ID = 'HAI_1_SIR')
WITH predicted_clabsi AS (
    SELECT *
    FROM {{ ref('int_hai_data_enriched') }}
    WHERE MEASURE_ID = 'HAI_1_SIR'
),

-- Calculate total unique facilities per state reporting SIR for CLABSI
facility_counts AS (
    SELECT 
        STATE,
        COUNT(DISTINCT FACILITY_ID) AS facility_count
    FROM predicted_clabsi
    GROUP BY STATE
),

-- Calculate the count of unique facilities per state classified as "Worse than the National Benchmark" based on SIR
worse_facilities AS (
    SELECT 
        STATE,
        COUNT(DISTINCT FACILITY_ID) AS worse_count
    FROM predicted_clabsi
    WHERE COMPARED_TO_NATIONAL = 'Worse than the National Benchmark'
    GROUP BY STATE
)

-- Join total facilities and worse-performing counts, then calculate the percentage of facilities worse than the national benchmark per state
SELECT 
    fc.STATE,
    fc.facility_count,
    wf.worse_count,
    ROUND(100.0 * wf.worse_count / fc.facility_count, 2) AS pct_worse_than_national
FROM facility_counts fc
LEFT JOIN worse_facilities wf
    ON fc.STATE = wf.STATE