# Healthcare Associated Infection Risk Analysis by ZIP Code and State

## Project Overview

This project analyzes and visualizes hospital infection risk across the United States by mapping average Standard Infection Ratios (SIR) for two key healthcare-associated infections:

- **CLABSI** (Central Line-Associated Bloodstream Infection)  
- **CAUTI** (Catheter-Associated Urinary Tract Infection)  

Using publicly available CMS data, this project aggregates SIR metrics at the ZIP code and state level, identifying geographic hotspots and trends. Interactive Tableau dashboards enable exploration of severity patterns with zoom and filter capabilities.

Due to their high acuity and significant infection risks, this analysis pays particular attention to the Midwest and Puerto Rico regions.

## Key Insights

- CLABSI issues are more widespread and affect more ZIP codes compared to CAUTI.  
- Geographic distribution of CLABSI shows clusters near the Mississippi River, California, the Midwest, South, and parts of the Northeast.  
- Puerto Rico shows significantly higher infection risks for both CLABSI and CAUTI compared to the continental US.  
- CAUTI infections are more localized, notably in the Midwest, Puerto Rico, Hawaii, and the Northeast.  
- Staffing shortages and equipment limitations contribute to elevated infection rates, especially in Puerto Rico and parts of the Midwest.

## Data Sources

- CMS API for data extraction of hospital infection data  
  - [Healthcare Associated Infections - Hospital](https://data.cms.gov/provider-data/dataset/77hc-ibv8#data-table)  
- Public reports and studies related to staffing shortages and infection control in healthcare facilities

## Technical Details

**ETL Pipeline:**  
- Data extraction via CMS API orchestrated using Apache Airflow  
- Data transformation and filtering performed in Python  
- Data loaded into Snowflake cloud data warehouse  
- dbt used for filtering, transforming, and creating intermediate and analytics views

**Analytics & Visualization:**  
- Tableau dashboards with interactive maps, color-coded severity gradients, and drill-down capabilities by ZIP code and state  

**Programming Languages & Tools:**  
- Python  
- SQL  
- Snowflake  
- dbt  
- Tableau  
- Apache Airflow
- ChatGPT (used for literature research and sourcing relevant reports)

## Use of AI Tools

During this project, I used ChatGPT as a research assistant to help source relevant reports, studies, and surveys related to healthcare-associated infections and regional healthcare challenges. By carefully evaluating these resources (including their timelines to ensure consistency with the infection data), I generated insights grounded in my clinical background and data analysis.

This approach allowed me to efficiently gather domain-specific information, which I then analyzed to generate insights and recommendations.
