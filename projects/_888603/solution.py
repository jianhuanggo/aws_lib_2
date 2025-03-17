from _util import _util_file as _util_file_
from sortedcontainers import SortedList
from _connect import _connect as _connect_
from _config import config as _config_

sql = """
with a as(
  select DISTINCT
    a.program_id,
    a.program_name as title,
    a.video_preview_url,
    import_id,
    landscape_img_url,
    start_ts::date as policy_start,
    end_ts::date as policy_end
    from datalake.content_info a 
    join datalake.content_availability s 
     on a.content_id = s.video_id
  where  s.country = 'US'
      --and is_episode is false
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
    date(ms) >= current_date - interval '1' month
    and country = 'US'
  group by 1
  order by 2 desc
)

    select DISTINCT
        a.program_id,
        nap_tvt,
        title,
        a.import_id,
        max(policy_start) as policy_start,
        max(policy_end) as policy_end
    from a
    left join top b 
      on a.program_id = b.program_id
    where landscape_img_url is null
    and policy_end >= current_date()
    and import_id <> 'the-exchange-original'
    group by 1,2,3,4
    order by b.nap_tvt desc

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

        result = object_directive_databrick.query(query_string=sql)
        return result.data_array if result and hasattr(result, 'data_array') else []


    # raw_data = get_data2()
    # formatted_data = []
    # for each_record in raw_data:
    #     program_id, nap_tvt, title, import_id, policy_start, policy_end = each_record
    #     formatted_data.append({"program_id": program_id, "nap_tvt": nap_tvt, "title": title, "import_id": import_id, "policy_start": policy_start, "policy_end": policy_end})
    #
    # _util_file_.json_to_csv("/Users/jian.huang/Downloads/888603_landscape_posters.csv", formatted_data)

    for each_record in _util_file_.csv_to_json("/Users/jian.huang/Downloads/888603_landscape_posters.csv"):
        if each_record.get("title").lower().startswith("strike"):
            print(each_record)

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