WITH base_returning_weekly as (
    SELECT distinct
        date_trunc('week', hs) as ws,
        user_alias,
        country,
        platform,
        platform_type,
        min(date_trunc('day', hs)) over (partition by user_alias, country) as first_ds_valid
    FROM `hive_metastore`.`tubidw_dev`.`user_byplatform_hourly` base
    WHERE 

    

    

    
      
      
    

    

    

          hs >= DATE_TRUNC( 'week', TO_DATE('2024-08-01', 'YYYY-MM-DD'))
      AND hs < DATE_TRUNC( 'week', TO_DATE('2023-08-01', 'YYYY-MM-DD'))

      

        + INTERVAL '168 days'

      

    


    AND date_trunc('week', (CASE WHEN device_first_view_ts > user_first_view_ts THEN COALESCE(user_first_view_ts, device_first_view_ts) ELSE device_first_view_ts END)) < date_trunc('week', hs)
),

retained_returning_weekly as (
  select
    base.ws,
    base.country,
    base.platform,
    base.platform_type,
    max(uph.hs) as _last_updated,
    hll_create_sketch((case when uph.hs between dateadd(hour, +24, base.first_ds_valid) and dateadd(hour, +168, base.first_ds_valid) then uph.user_alias else NULL end)) as retained_d1to7_sketch,
    hll_create_sketch((case when uph.hs between dateadd(hour, +24, base.first_ds_valid) and dateadd(hour, +672, base.first_ds_valid) then uph.user_alias else NULL end)) as retained_d1to28_sketch,
    hll_create_sketch((case when uph.hs between dateadd(hour, +672, base.first_ds_valid) and dateadd(hour, +1344, base.first_ds_valid) then uph.user_alias else NULL end)) as retained_d28to56_sketch,
    hll_create_sketch((case when uph.hs between dateadd(hour, +1344, base.first_ds_valid) and dateadd(hour, +2016, base.first_ds_valid) then uph.user_alias else NULL end)) as retained_d56to84_sketch,
    hll_create_sketch((case when uph.hs between dateadd(hour, +2016, base.first_ds_valid) and dateadd(hour, +2688, base.first_ds_valid) then uph.user_alias else NULL end)) as retained_d84to112_sketch,
    hll_create_sketch((case when uph.hs between dateadd(hour, +2688, base.first_ds_valid) and dateadd(hour, +3360, base.first_ds_valid) then uph.user_alias else NULL end)) as retained_d112to140_sketch,
    hll_create_sketch((case when uph.hs between dateadd(hour, +3360, base.first_ds_valid) and dateadd(hour, +4032, base.first_ds_valid) then uph.user_alias else NULL end)) as retained_d140to168_sketch,
    hll_create_sketch((base.user_alias)) as denominator_sketch
  from base_returning_weekly base
  left join `hive_metastore`.`tubidw_dev`.`user_byplatform_hourly` uph
    on base.user_alias=uph.user_alias
    and base.country=uph.country
    and base.platform=uph.platform
    and base.platform_type=uph.platform_type
    and uph.hs between dateadd('hour', +24, base.first_ds_valid) and dateadd('hour', +4032, base.first_ds_valid)
  group by
    base.ws,
    base.country,
    base.platform,
    base.platform_type
),

base_new_weekly as (
  SELECT distinct
    date_trunc('week', (CASE WHEN device_first_view_ts > user_first_view_ts THEN COALESCE(user_first_view_ts, device_first_view_ts) ELSE device_first_view_ts END)) as ws,
    user_alias,
    country,
    platform,
    platform_type,
    (CASE WHEN device_first_view_ts > user_first_view_ts THEN COALESCE(user_first_view_ts, device_first_view_ts) ELSE device_first_view_ts END) as first_ds_valid
  FROM `hive_metastore`.`tubidw_dev`.`user_byplatform_hourly` base
  WHERE 

    

    

    
      
      
    

    

    

          hs >= DATE_TRUNC( 'week', TO_DATE('2024-08-01', 'YYYY-MM-DD'))
      AND hs < DATE_TRUNC( 'week', TO_DATE('2023-08-01', 'YYYY-MM-DD'))

      

        + INTERVAL '168 days'

      

    


),

retained_new_weekly as (
  select
    base.ws,
    base.country,
    base.platform,
    base.platform_type,
    max(uph.hs) as _last_updated,
    hll_create_sketch((case when uph.hs between dateadd(hour, +24, base.first_ds_valid) and dateadd(hour, +168, base.first_ds_valid) then uph.user_alias else NULL end)) as retained_d1to7_sketch,
    hll_create_sketch((case when uph.hs between dateadd(hour, +24, base.first_ds_valid) and dateadd(hour, +672, base.first_ds_valid) then uph.user_alias else NULL end)) as retained_d1to28_sketch,
    hll_create_sketch((case when uph.hs between dateadd(hour, +672, base.first_ds_valid) and dateadd(hour, +1344, base.first_ds_valid) then uph.user_alias else NULL end)) as retained_d28to56_sketch,
    hll_create_sketch((case when uph.hs between dateadd(hour, +1344, base.first_ds_valid) and dateadd(hour, +2016, base.first_ds_valid) then uph.user_alias else NULL end)) as retained_d56to84_sketch,
    hll_create_sketch((case when uph.hs between dateadd(hour, +2016, base.first_ds_valid) and dateadd(hour, +2688, base.first_ds_valid) then uph.user_alias else NULL end)) as retained_d84to112_sketch,
    hll_create_sketch((case when uph.hs between dateadd(hour, +2688, base.first_ds_valid) and dateadd(hour, +3360, base.first_ds_valid) then uph.user_alias else NULL end)) as retained_d112to140_sketch,
    hll_create_sketch((case when uph.hs between dateadd(hour, +3360, base.first_ds_valid) and dateadd(hour, +4032, base.first_ds_valid) then uph.user_alias else NULL end)) as retained_d140to168_sketch,
    hll_create_sketch((base.user_alias)) as denominator_sketch
  from base_new_weekly base
  left join `hive_metastore`.`tubidw_dev`.`user_byplatform_hourly` uph
    on base.user_alias=uph.user_alias
    and base.country=uph.country
    and base.platform=uph.platform
    and base.platform_type=uph.platform_type
    and uph.hs between dateadd('hour', +24, base.first_ds_valid) and dateadd('hour', +4032, base.first_ds_valid)
  group by base.ws, base.country, base.platform, base.platform_type
)

select
  n.ws || n.platform || n.platform_type || n.country as _id,
  n._last_updated as _last_updated,
  n.ws,
  n.platform,
  n.platform_type,
  n.country,
  n.retained_d1to7_sketch as new_retained_d1to7_sketch,
  n.retained_d1to28_sketch as new_retained_d1to28_sketch,
  n.retained_d28to56_sketch as new_retained_d28to56_sketch,
  n.retained_d56to84_sketch as new_retained_d56to84_sketch,
  n.retained_d84to112_sketch as new_retained_d84to112_sketch,
  n.retained_d112to140_sketch as new_retained_d112to140_sketch,
  n.retained_d140to168_sketch as new_retained_d140to168_sketch,
  n.denominator_sketch as new_retention_denominator_sketch,

  w.retained_d1to7_sketch as returning_retained_d1to7_week_sketch,
  w.retained_d1to28_sketch as returning_retained_d1to28_week_sketch,
  w.retained_d28to56_sketch as returning_retained_d28to56_week_sketch,
  w.retained_d56to84_sketch as returning_retained_d56to84_week_sketch,
  w.retained_d84to112_sketch as returning_retained_d84to112_week_sketch,
  w.retained_d112to140_sketch as returning_retained_d112to140_week_sketch,
  w.retained_d140to168_sketch as returning_retained_d140to168_week_sketch,
  w.denominator_sketch as returning_retention_denominator_week_sketch

from retained_new_weekly n
join retained_returning_weekly w
  on n.ws=w.ws
  and n.platform=w.platform
  and n.platform_type=w.platform_type
  and n.country=w.country
where 

    

    

    
      
      
    

    

    

          n.ws >= DATE_TRUNC( 'week', TO_DATE('2024-08-01', 'YYYY-MM-DD'))
      AND n.ws < DATE_TRUNC( 'week', TO_DATE('2023-08-01', 'YYYY-MM-DD'))

      

    



