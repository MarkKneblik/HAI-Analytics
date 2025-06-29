-- Creates or replaces the HAI_DATA table in the RAW schema
-- This table stores healthcare-associated infection data with:
--   - Facility identifiers and location details (ID, name, state, zip)
--   - Measure identifiers and descriptions
--   - Comparison status against national benchmarks
--   - Numeric score representing the measure result

CREATE OR REPLACE TABLE HAI_DATA(
    FACILITY_ID VARCHAR(10) NOT NULL,
    FACILITY_NAME VARCHAR(90) NOT NULL,
    STATE VARCHAR(2) NOT NULL,
    ZIP_CODE VARCHAR(5) NOT NULL,
    MEASURE_ID VARCHAR(20) NOT NULL,
    MEASURE_NAME VARCHAR(100) NOT NULL,
    COMPARED_TO_NATIONAL VARCHAR(50) NOT NULL,
    SCORE FLOAT
);