{{ config(materialized='incremental',unique_key='invoiceid') }}

SELECT  invoice.*
       ,'{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S")}}'::timestamp as dbt_time
FROM {{source('stg','invoice')}}
WHERE 1=1
 {% if is_incremental() %}
 and last_update::timestamp > (select max(last_update) from {{this}})
 {% endif %}