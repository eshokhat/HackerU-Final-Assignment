{{ config(materialized='table',unique_key='employeeid') }}

SELECT   emp.*
        ,db.department_name
        ,db.budget
        ,EXTRACT(YEAR FROM age(CURRENT_DATE, emp.hiredate)) AS years_in_company
        ,left(substring(email, position('@' in email) + 1),position('.' in substring(email, position('@' in email) + 1))-1) as domain
        ,case when emp.employeeid in (select distinct reportsto from stg.employee) then 1 
             else 0 
                end as is_manager
        ,'{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S") }}'::timestamp as dbt_time
FROM {{source('stg','employee')}} emp
     JOIN {{source('stg','department_budget')}} db on emp.departmentid = db.department_id