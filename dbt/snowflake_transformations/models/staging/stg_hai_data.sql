-- models/staging/stg_hai_data.sql
select * from {{ source('HAI_DATABASE', 'HAI_DATA') }}