
select  pl.playlistid as playlist_id
       ,pl.name as playlist_name
       ,pl.last_update as last_update
       ,plt.trackid as track_id
       ,plt.last_update as track_last_update
       ,'{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S") }}'::timestamp as dbt_time
from {{source('stg','playlist')}} as pl
     left join {{source('stg','playlisttrack')}} plt on pl.playlistid = plt.playlistid
where 1=1
 {% if is_incremental() %}
 and plt.last_update::timestamp > (select max(pl.last_update) from {{this}})
 {% endif %}