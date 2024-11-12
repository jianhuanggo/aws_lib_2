import json

from _connect import _connect as _connect_
from typing import Dict
from _config import config as _config_
from inspect import currentframe


def test():
    _object_s3 = _connect_.get_object("awss3")
    print(_object_s3. list_buckets())
    # print(_object_s3.create_presigned_url("s3://pg-share-out-001/aws.jpg", expiration=604800))



def run():
    from task.analysis import analysis
    # analysis.find_similar_directory("adserver_metric_daily", "/Users/jian.huang/projects/dw/tubibricks/models")
    col_lst, groupby_lst, orderby_lst = analysis.get_table_info("/Users/jian.huang/anaconda3/envs/aws_lib/pg-aws-lib/task/801954")
    gen_group_list = analysis.generate_id_columns(col_lst, groupby_lst)
    sql_group_list = """
    {% set id_columns = ["ds", "platform", "country", "city", "subdivision", "dma", "language", "autoplay_on", "content_genres", "content_ratings", "content_type", "device_type", "revenue_vertical", "ramp_id_type", "identity_data_source", "ad_opportunity_reason", "opt_out", "is_coppa", "coppa_enabled", "Ad_break_position", "user_gender", "targeted_seq_pos", "device_deal", "remnant_status", "autoplay_idx", "tracking_mode", "app_mode", "Logged_status", "postal_code", "user_age"] %}
    """
    print(analysis.string_compare(gen_group_list, sql_group_list.strip()))


# def run1():
#     from _engine import _subprocess
#     command_line.run_command("ls -rlt", env_vars={
#         "MODEL_NAME": "adserver_metric_daily",
#         "MODEL_DIR": "adserver",
#         "TUBIBRICKS_HOME": "/Users/jian.huang/projects/dw/tubibricks"}
#     )

def run2(vars_dict: Dict):
    """

    Args:
        vars_dict:

    Returns:

    testing:

    #
    #
    # # from _engine._airflow import AirflowRunner
    # # commands = ["ls"]
    # # commands = ["ls", "echo xxx444", "ls -lrt"]
    # # shell_runner = AirflowRunner()
    # # execute_command(shell_runner, commands)
    #

    """

    # from _util import _util_directory as _util_directory_
    # print(_util_directory_.dirs_in_dir("/Users/jian.huang/anaconda3/envs/aws_lib/pg-aws-lib/task"))
    #
    # exit(0)
    # from task import task_completion
    #
    # task_completion._d123_process_sql_v1({})
    #
    # exit(0)

    # task_completion.get_task.get_task(800000)

    # from _management._meta import _inspect_module
    #
    # sql_file = _inspect_module.load_module_from_path("/Users/jian.huang/anaconda3/envs/aws_lib/pg-aws-lib/task/800000/jian_poc_model.py", "test")
    # print(sql_file.SQL)
    #
    # exit(0)


    from _engine._subprocess import ShellRunner
    from _engine._command_protocol import execute_command_from_dag

    from _pattern_template.template_v1 import model_history_load_template
    from _engine import _process_flow

    t_task = _process_flow.process_template(model_history_load_template)
    shell_runner = ShellRunner()
    execute_command_from_dag(shell_runner, t_task.tasks)
























    # filtered_vars = {name: value for name, value in locals().items() if name.startswith()}



def run_search():
    from _search import _semantic_search_faiss
    ss = _semantic_search_faiss.SemanticSearchFaiss("error_bank")

    _error_msg = {
        "process_name": "_subprocess",
        "error_type": "normal",
        "recovery_type": "normal",
        "recovery_method": ""
    }




    error_list = ["ValueError: not enough values to unpack (expected 2, got 1)",
                  "Error occurred: fatal: not a git repository (or any of the parent directories): .git",
                  "Error in 3 validation errors for DictValiatorModelAllString",
                  "Error occurred: error: pathspec 'jian_dbt_poc' did not match any file(s) known to git"
                  ]

    # for error in error_list:
    #     ss.add_index(error, _error_msg)

    # ss.add_index("this is a test")
    # # print(ss.search("is there a test there"))

    result = ss.search("ValueError: not enough values to unpack", k=3, threshold=10)
    print(result)


    exit(0)






    # class SemanticSearchFaiss:
    #     def __init__(self):
    #         self.model = SentenceTransformer('paraphrase-MiniLM-L6-v2')
    #
    #     def encode_message(self, message: str):
    #         embeddings = self.model.encode(message)
    #         embedding_shape = embeddings.shape
    #         print(embedding_shape)
    print(ss.load_index("/Users/jian.huang/anaconda3/envs/aws_lib/pg-aws-lib/search_index.json"))

"""
"""
def run10(search_string: str,
          replace_string: str):


    border_view_buffer = 20
    str_len = len(search_string)
    from _util import _util_file
    for each_file in _util_file.files_in_dir(
            "/Users/jian.huang/projects/dw/tubibricks/src/adserver_metric_daily/models/adserver"):
        print(each_file)
        notebook_file_str = _util_file.identity_load_file(each_file)

        index = notebook_file_str.find(search_string)
        if index <= 0:
            print("search string is not found in this file {each_file}")

        if replace_string:
            print(
                f"before the change!!! {notebook_file_str[index - border_view_buffer: index + str_len + border_view_buffer]}")
            notebook_file_str = notebook_file_str.replace(search_string, replace_string)
            _util_file.identity_write_file(each_file, notebook_file_str)
            notebook_file_str = _util_file.identity_load_file(each_file)

            print(
                f"after the change {notebook_file_str[index - border_view_buffer: index + str_len + border_view_buffer]}")




# def run_redshift():
#     from

def run_test1():

    directive_object = _connect_.get_directive("image_to_text")
    directive_object.run(**{"filepath": "/Users/jian.huang/Downloads/test.png"})
    # class DirectiveImage_to_text(metaclass=_meta_.MetaDirective):
    #     def __init__(self, config: _config_.ConfigSingleton = None, logger: Log = None):
    #         self._config = config if config else _config_.ConfigSingleton()
    #
    #     @_common_.exception_handler
    #     def run(self, *arg, **kwargs) -> str:
    #         return self._implementation_trocr(kwargs.get("filepath"))

def latest_template():
    from _engine._subprocess import ShellRunner
    from _engine._command_protocol import execute_command_from_dag

    from _pattern_template._process_template import _process_template

    # t_task = _process_template.process_template("/Users/jian.huang/anaconda3/envs/aws_lib/pg-aws-lib/_pattern_template/tubibricks_deployment_only.yaml", "config_dev")
    t_task = _process_template.process_template("config_prod", "/Users/jian.huang/anaconda3/envs/aws_lib/pg-aws-lib/_pattern_template/tubibricks_history_load_template.yaml", )

    shell_runner = ShellRunner()
    execute_command_from_dag(shell_runner, t_task.tasks)

def databricks_sdk():
    def monitoring_job(db_object,
                       user_name: str,
                       job_id: int):

        from time import sleep
        from pprint import pprint
        _WAIT_TIME_INTERVAL_ = 60
        #
        # pprint(db_object.job_run_now_job_id(job_id=job_id))

        # print([(each_run[0], each_run[1], each_run[2], each_run[3]) for each_run in db_object.get_job_run_id(job_id = job_id) if each_run[3] == "RUNNING"])
        #


        while running_job := [(each_run[0], each_run[1], each_run[2], each_run[3]) for each_run in db_object.get_job_run_id(job_id = job_id) if each_run[3] == "RUNNING"]:
            # running_job = [(each_run[0], each_run[1], each_run[2], each_run[3]) for each_run in db_object.get_job_run_id(job_id = job_id) if each_run[3] == "RUNNING")
            job_id, job_run_id, original_job_run_id, status = running_job[0]
            if status == "RUNNING":
                _common_.info_logger(f"job_id {job_id} is running, please wait...")
                sleep(_WAIT_TIME_INTERVAL_)
            elif status == "INTERNAL_ERROR":
                _common_.info_logger(f"job_id {job_id} is encountered internal error, starting retry...")
                pprint(db_object.job_repair_now_job_id(job_run_id=job_run_id))
            else:
                _common_.info_logger(f"job_id {job_id} is {status}, exit waiting.")
                break


    db_object = _connect_.get_directive("databrick_sdk", "config_dev")
    # print(db_object.list_run_active(user_name="jian.huang@tubi.tv"))


    # pprint(ds_object.get_job("920730616251469"))
    # pprint(list(ds_object.list_job(job_name="[dev jian_huang] revenue_bydevice_daily"))[0].job_id)
    from _common import _common as _common_
    jobs = list(db_object.get_job_id_by_name(job_name="[dev jian_huang] revenue_bydevice_daily"))
    if len(jobs) > 1:
        _common_.error_logger(currentframe().f_code.co_name,
                              f"there are multiple jobs found with the same name, please check",
                              logger=None,
                              mode="error",
                              ignore_flag=False)
    monitoring_job(db_object, "jian.huang@tubi.tv", jobs[0])


    "each_job.status.termination_details.message"
    from datetime import datetime
    # status = sorted([(each_job.job_id,
    #            each_job.run_id,
    #            each_job.original_attempt_run_id,
    #            datetime.fromtimestamp(each_job.end_time/1000),
    #            each_job.state.life_cycle_state.value,
    #            ) for each_job in db_object.list_runs_by_jobid(jobid=jobs[0])], reverse=True, key=lambda x: x[3])
    # from pprint import pprint
    # pprint(status)
    # exit(0)



    # def get_run(run_id)
    #
    #
    #
    # def get_run_output(self, run_id: int) -> RunOutput:






    # pprint(list(db_object.list_runs_by_jobid(jobs[0])))
    #
    #
    #
    # def job_repair_now_job_id(self, job_run_id: int, *arg, **kwargs):
    #     return self.client.jobs.repair_run(run_id=job_run_id)
    # exit(0)
    # print(db_object.list_runs(user_name="jian.huang@tubi.tv"))
    #
    # exit(0)
    #
    # # db_object.job_run_now_job_id(jobs[0])
    # monitoring_job(db_object, "jian.huang@tubi.tv", jobs[0])
    #






    # print(object.list_catalog())


def redshift():
    aws_object = _connect_.get_object("Redshift")
    aws_object.change_account_by_profile_name(profile_name="main-data-eng-admin", aws_region="us-east-2")
    # aws_object.switch_aws_account(account_name = "main-data-eng-admin")
    print([each_record.get("ClusterIdentifier") for each_record in aws_object._client.describe_clusters().get("Clusters", [])])









if __name__ == '__main__':
    databricks_sdk()
    exit(0)
    redshift()
    exit(0)


    latest_template()
    exit(0)
    run_test1()
    exit(0)

    # run10("merge into `hive_metastore`.`tubidw`.`adserver_metric_daily`",
    #       "merge into `hive_metastore`.`tubidw_dev`.`adserver_metric_daily`")
    # exit(0)
    # run_search()
    # exit(0)


    run2({})
    exit(0)
    run1()
    exit(0)
    test()
