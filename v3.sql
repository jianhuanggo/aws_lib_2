with old_data as (
select 
sum(coalesce(gross_revenue,0)) as gross_revenue,
sum(coalesce(total_revenue,0)) as total_revenue,
sum(coalesce(gross_vod_revenue,0)) as gross_vod_revenue,
sum(coalesce(total_vod_revenue,0)) as total_vod_revenue,
sum(coalesce(gross_linear_revenue,0)) as gross_linear_revenue,
sum(coalesce(total_linear_revenue,0)) as total_linear_revenue,
sum(coalesce(unbudgeted_revenue,0)) as unbudgeted_revenue,
sum(coalesce(unclassified_revenue,0)) as unclassified_revenue,
sum(coalesce(budgeted_revenue,0)) as budgeted_revenue 
from hive_metastore.tubidw.revenue_bydevice_daily
  where TO_DATE(ds, 'yyyy-MM-dd') >= DATE_TRUNC("day", TO_DATE('2024-08-01', "yyyy-MM-dd")) AND TO_DATE(ds, "yyyy-MM-dd") < DATE_TRUNC("day", TO_DATE('2024-08-08', "yyyy-MM-dd")) 
 group by ds  ), new_data as (
select 
sum(coalesce(gross_revenue,0)) as gross_revenue,
sum(coalesce(total_revenue,0)) as total_revenue,
sum(coalesce(gross_vod_revenue,0)) as gross_vod_revenue,
sum(coalesce(total_vod_revenue,0)) as total_vod_revenue,
sum(coalesce(gross_linear_revenue,0)) as gross_linear_revenue,
sum(coalesce(total_linear_revenue,0)) as total_linear_revenue,
sum(coalesce(unbudgeted_revenue,0)) as unbudgeted_revenue,
sum(coalesce(unclassified_revenue,0)) as unclassified_revenue,
sum(coalesce(budgeted_revenue,0)) as budgeted_revenue 
from hive_metastore.tubidw_dev.revenue_bydevice_daily
  where TO_DATE(ds, 'yyyy-MM-dd') >= DATE_TRUNC("day", TO_DATE('2024-08-01', "yyyy-MM-dd")) AND TO_DATE(ds, "yyyy-MM-dd") < DATE_TRUNC("day", TO_DATE('2024-08-08', "yyyy-MM-dd")) 
 group by ds  )
select 
t1.gross_revenue ,t2.gross_revenue ,t1.gross_revenue / greatest(0.01, t2.gross_revenue) * 100, 
t1.total_revenue ,t2.total_revenue ,t1.total_revenue / greatest(0.01, t2.total_revenue) * 100, 
t1.gross_vod_revenue ,t2.gross_vod_revenue ,t1.gross_vod_revenue / greatest(0.01, t2.gross_vod_revenue) * 100, 
t1.total_vod_revenue ,t2.total_vod_revenue ,t1.total_vod_revenue / greatest(0.01, t2.total_vod_revenue) * 100, 
t1.gross_linear_revenue ,t2.gross_linear_revenue ,t1.gross_linear_revenue / greatest(0.01, t2.gross_linear_revenue) * 100, 
t1.total_linear_revenue ,t2.total_linear_revenue ,t1.total_linear_revenue / greatest(0.01, t2.total_linear_revenue) * 100, 
t1.unbudgeted_revenue ,t2.unbudgeted_revenue ,t1.unbudgeted_revenue / greatest(0.01, t2.unbudgeted_revenue) * 100, 
t1.unclassified_revenue ,t2.unclassified_revenue ,t1.unclassified_revenue / greatest(0.01, t2.unclassified_revenue) * 100, 
t1.budgeted_revenue ,t2.budgeted_revenue ,t1.budgeted_revenue / greatest(0.01, t2.budgeted_revenue) * 100
from hive_metastore.tubidw.revenue_bydevice_daily t1 inner join hive_metastore.tubidw_dev.revenue_bydevice_daily t2 using (ds)
