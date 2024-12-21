import os

import click
from inspect import currentframe
from datetime import datetime
from time import sleep
from typing import List
from logging import Logger as Log
from _common import _common as _common_
from _config import config as _config_
from _connect import _connect as _connect_
from _util import _util_file as _util_file_


@click.command()
@click.option('--profile_name', required=True, type=str)
@click.option('--notebook_filepath', required=True, type=str)
@click.option('--job_filepath', required=True, type=str)
@click.option('--model_dir', required=True, type=str)
def enhance_notebook(profile_name: str,
                     notebook_filepath: str,
                     job_filepath: str,
                     model_dir: str,
                     logger: Log = None):

    """ this script monitors databricks workflow job and restarts if necessary

    Args:

        notebook_filepath: notebook filepath
        profile_name: profile, contains environment variables regarding to databricks environment (config_dev, config_prod, config_stage)
        job_filepath: job filepath
        model_name: model name
        model_dir: mode dir
        logger: logging object

    Returns:
        return true if successful otherwise return false

    """

    _config = _config_.ConfigSingleton(profile_name=profile_name)

    if profile_name:
        _config.config["PROFILE_NAME"] = profile_name
    elif "PROFILE_NAME" in os.environ:
        _config.config["PROFILE_NAME"] = os.environ.get("PROFILE_NAME")

    if notebook_filepath:
        _config.config["NOTEBOOK_FILEPATH"] = notebook_filepath
    elif "NOTEBOOK_FILEPATH" in os.environ:
        _config.config["NOTEBOOK_FILEPATH"] = os.environ.get("NOTEBOOK_FILEPATH")

    if job_filepath:
        _config.config["JOB_FILEPATH"] = job_filepath
    elif "JOB_FILEPATH" in os.environ:
        _config.config["JOB_FILEPATH"] = os.environ.get("JOB_FILEPATH")

    if model_dir:
        _config.config["MODEL_DIR"] = model_dir
    elif "MODEL_DIR" in os.environ:
        _config.config["MODEL_DIR"] = os.environ.get("MODEL_DIR")

    _common_.info_logger(f"profile_name: {profile_name}", logger=logger)
    _common_.info_logger(f"notebook_filepath: {notebook_filepath}", logger=logger)
    _common_.info_logger(f"job_filepath: {job_filepath}", logger=logger)
    _common_.info_logger(f"model_dir: {model_dir}", logger=logger)

    try:
        for each_dir in _util_file_.dir_in_dir(notebook_filepath):
            print(each_dir)
            for each_file in _util_file_.files_in_dir(os.path.join(each_dir, "models", model_dir)):
                print(each_file)
                from pprint import pprint
                each_notebook = _util_file_.json_load(each_file)
                new_sql_content = []
                for each_sql_block in each_notebook.get("cells", [{}])[0].get("source", []):
                    pprint(each_sql_block)
                    block_content = each_sql_block
                    # print(type(block_content))

                    pprint(_util_file_.json_load(job_filepath))
                    for old_table, new_table in _util_file_.json_load(job_filepath).get("table_name", {}).items():
                        block_content = block_content.replace(old_table, new_table)
                    new_sql_content.append(block_content)
                pprint(new_sql_content)
                if new_sql_content:
                    each_notebook["cells"][0]["source"] = new_sql_content
                    file_name, file_extension = os.path.splitext(each_file)
                    # _util_file_.json_dump(file_name + "_modifiled" + file_extension, each_notebook)
                    _util_file_.json_dump(each_file, each_notebook)

    except Exception as err:
        _common_.error_logger(currentframe().f_code.co_name,
                              err,
                              logger=logger,
                              mode="error",
                              ignore_flag=False)

if __name__ == '__main__':
    enhance_notebook()