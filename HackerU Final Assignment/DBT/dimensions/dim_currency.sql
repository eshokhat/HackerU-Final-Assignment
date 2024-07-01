{{ config(materialized='table',unique_key='date_rate') }}

SELECT  distinct invoicedate as date_rate
       ,exchange_rate.ils as ils
FROM {{source('stg','invoice')}} as invoice
	 left join {{source('stg','exchange_rate')}} as exchange_rate
        on invoice.invoicedate = exchange_rate.date_rate
ORDER BY date_rate asc