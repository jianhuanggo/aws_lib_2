from _util import _util_file as _util_file_
from sortedcontainers import SortedList

def solution(personalized_gn_id_filepath: str,
             tubi_month_title_filepath: str):

    personalized_gn_ids = _util_file_.csv_to_json(personalized_gn_id_filepath)


    unique_personalized_gn_id = {v for each_record in personalized_gn_ids for _, v in each_record.items()}
    tubi_feb_month_titles = _util_file_.csv_to_json(tubi_month_title_filepath)

    _2000k_titles_in_personalized_gn_id = []

    s = SortedList()
    for each_record in tubi_feb_month_titles:
        s.add((- each_record["total_tvt_hr"], each_record["content_name"], each_record["gracenote_id"]))

        if len(s) > 2000:
            s.pop()

    _2k_titles_in_personalized_gn_id = [{"total_tvt_hr": - each_record[0], "content_name": each_record[1], "gracenote_id": each_record[2]} for each_record in s if each_record[2] in unique_personalized_gn_id]
    _util_file_.json_to_csv("/Users/jian.huang/Downloads/matched_tubi_2k_title_gracenote_image_personalization_package.csv", _2k_titles_in_personalized_gn_id)

    print(len(_2k_titles_in_personalized_gn_id))
    print(len(_2k_titles_in_personalized_gn_id) / 2000)


    s = SortedList()
    for each_record in tubi_feb_month_titles:
        s.add((- each_record["total_tvt_hr"], each_record["content_name"], each_record["gracenote_id"]))
        if len(s) > 5000:
            s.pop()


    _5k_titles_in_personalized_gn_id = [{"total_tvt_hr": - each_record[0], "content_name": each_record[1], "gracenote_id": each_record[2]} for each_record in s if each_record[2] in unique_personalized_gn_id]
    _util_file_.json_to_csv("/Users/jian.huang/Downloads/matched_tubi_5k_title_gracenote_image_personalization_package.csv", _5k_titles_in_personalized_gn_id)

    print(len(_5k_titles_in_personalized_gn_id))
    print(len(_5k_titles_in_personalized_gn_id) / 5000)

