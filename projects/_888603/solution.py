from _util import _util_file as _util_file_
from sortedcontainers import SortedList
from _connect import _connect as _connect_
from _config import config as _config_

sql = """
        with a as(
  select DISTINCT
    a.program_id,
    a.title,
    a.video_preview_url,
    import_id,
    landscape_img_url,
    start_ts::date as policy_start,
    end_ts::date as policy_end
    from datalake.content_info a 
    join datalake.content_availability s 
     on a.content_id = s.video_id
  where  s.country = 'US'
      and is_episode is false
      and s.active is true
      and s.policy is true
      and s.contains_tubitv is true
),

top as (
  select
    a.program_id, 
    sum(non_autoplay_tvt_sec * 1.0 / 3600) as nap_tvt
  from
    tubidw_dev.content_monthly_bycountry a
  where
    date(ms) >= '2024-08-01'
    and country = 'US'
  group by 1
  order by 2 desc
  limit 1000
)

select DISTINCT
    a.program_id,
    title,
    a.import_id,
    min(policy_start) as policy_start,
    max(policy_end) as policy_end,
    case when b.program_id is not null then 'Top 1000 Title' else 'null' end as top,
    nap_tvt as non_autoplay_tvt
from a
join top b
    on a.program_id = b.program_id
where landscape_img_url is null
  and policy_end >= current_date()
  and import_id <> 'the-exchange-original'
group by 1,2,3,6,7
order by 7 desc, 2
        """

def solution(csv_filepath: str):
    # print(csv_filepath)
    # exit(0)

    def get_data():
        profile_name = "config_prod"
        _config = _config_.ConfigSingleton(profile_name=profile_name)
        object_api_databrick = _connect_.get_api("databrickscluster", profile_name)


        result_pd = object_api_databrick.raw_query(query_string=sql).toPandas()
        print(result_pd)

    def get_data2():
        profile_name = "config_prod"
        _config = _config_.ConfigSingleton(profile_name=profile_name)
        object_directive_databrick = _connect_.get_directive("databricks_sdk", profile_name)

        result_pd =  object_directive_databrick.query(query_string=sql)
        print(result_pd)

    get_data2()

    exit(0)

    print
    raw_data = _util_file_.csv_to_json(csv_filepath)
    if len(raw_data) > 0:
        print(list(raw_data[0].keys()))
    #
    # for each_record in raw_data:
    #     if each_record.keys() != ['time']:
    #
    #     # if each_record.get()