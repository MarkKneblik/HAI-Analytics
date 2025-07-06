# Healthcare Infection Risk Analysis by ZIP Code

## Project Overview

This project analyzes and visualizes hospital infection risk across the United States by mapping average Standard Infection Ratios (SIR) for two key healthcare-associated infections:

- **CLABSI** (Central Line-Associated Bloodstream Infection)  
- **CAUTI** (Catheter-Associated Urinary Tract Infection)  

Using publicly available CMS data, this project aggregates SIR metrics at the ZIP code level, identifying geographic hotspots and trends. Interactive Tableau dashboards enable exploration of severity patterns with zoom and filter capabilities.

## Key Insights

- CLABSI issues are more widespread and affect more ZIP codes compared to CAUTI.  
- Geographic distribution of CLABSI shows clusters near the Mississippi River, California, the Midwest, South, and parts of the Northeast.  
- Puerto Rico shows significantly higher infection risks for both CLABSI and CAUTI compared to the continental US.  
- CAUTI infections are more localized, notably in the Midwest, Puerto Rico, Hawaii, and Northeast.  
- Staffing shortages and equipment limitations contribute to elevated infection rates, especially in Puerto Rico and parts of the Midwest.

## Data Sources

- CMS Hospital Infection Data (Standard Infection Ratios by ZIP code)  
- CMS API for data extraction  
- Public reports and studies related to staffing shortages and infection control in healthcare facilities

## Technical Details

**ETL Pipeline:**  
- Data extraction via CMS API orchestrated using Apache Airflow  
- Data transformation and filtering performed in Python  
- Data loaded into Snowflake cloud data warehouse  
- dbt used to create intermediate and analytics views  

**Analytics & Visualization:**  
- Tableau dashboards with interactive maps, color-coded severity gradients, and drill-down capabilities by ZIP code and state  

**Programming Languages & Tools:**  
Python, SQL, Snowflake, dbt, Tableau, Apache Airflow
