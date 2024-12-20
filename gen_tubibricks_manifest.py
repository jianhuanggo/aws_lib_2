import os

import click
from datetime import datetime
from logging import Logger as Log
from _common import _common as _common_
from _config import config as _config_
from _connect import _connect as _connect_
from _util import _util_file as _util_file_

@click.command()
@click.option('--model_name', required=True, type=str)
@click.option('--model_description', required=True, type=str)
@click.option('--manifest_filepath', required=True, type=str)
@click.option('--table_def_filepath', required=True, type=str)
@click.option('--profile_name', required=True, type=str)
@click.option('--output_filepath', required=True, type=str)
def gen_tubibricks_manifest(model_name: str,
                            model_description: str,
                            manifest_filepath: str,
                            table_def_filepath: str,
                            profile_name: str,
                            output_filepath: str,
                            logger: Log = None) -> bool:

    """ this script reads create table statement and generate column comment manifest

    Args:
        model_name: the job name
        model_description: the description of the model
        manifest_filepath: input manifest filepath
        table_def_filepath: table definition, used to extract column names
        output_filepath: output file path
        profile_name: profile, contains environment variables regarding to databricks environment (config_dev, config_prod, config_stage)
        logger: logging object

    Returns:
        return true if successful otherwise return false

    """


    _config = _config_.ConfigSingleton(profile_name=profile_name)

    if model_name:
        _config.config["MODEL_NAME"] = model_name
    elif "MODEL_NAME" in os.environ:
        _config.config["MODEL_NAME"] = os.environ.get("MODEL_NAME")

    if model_description:
        _config.config["MODEL_DESCRIPTION"] = model_description
    elif "MODEL_DESCRIPTION" in os.environ:
        _config.config["MODEL_DESCRIPTION"] = os.environ.get("MODEL_DESCRIPTION")

    if manifest_filepath:
        _config.config["MANIFEST_FILEPATH"] = manifest_filepath
    elif "MANIFEST_FILEPATH" in os.environ:
        _config.config["MANIFEST_FILEPATH"] = os.environ.get("MANIFEST_FILEPATH")

    if table_def_filepath:
        _config.config["TABLE_DEF_FILEPATH"] = table_def_filepath
    elif "TABLE_DEF_FILEPATH" in os.environ:
        _config.config["TABLE_DEF_FILEPATH"] = os.environ.get("TABLE_DEF_FILEPATH")

    if model_name:
        _config.config["PROFILE_NAME"] = model_name
    elif "PROFILE_NAME" in os.environ:
        _config.config["PROFILE_NAME"] = os.environ.get("PROFILE_NAME")

    if output_filepath:
        _config.config["OUTPUT_FILEPATH"] = output_filepath
    elif "OUTPUT_FILEPATH" in os.environ:
        _config.config["OUTPUT_FILEPATH"] = os.environ.get("OUTPUT_FILEPATH")

    _common_.info_logger(f"model_name: {_config.config.get('MODEL_NAME')}")
    _common_.info_logger(f"model_name: {_config.config.get('MODEL_DESCRIPTION')}")
    _common_.info_logger(f"model_name: {_config.config.get('MANIFEST_FILEPATH')}")
    _common_.info_logger(f"table_def_filepath: {_config.config.get('TABLE_DEF_FILEPATH')}")
    _common_.info_logger(f"profile_name: {_config.config.get('PROFILE_NAME')}")
    _common_.info_logger(f"output_filepath: {_config.config.get('OUTPUT_FILEPATH')}")

    _common_.info_logger(f"start time:{datetime.now()}")

    print(_config.config)
    print(profile_name)

    object_directive = _connect_.get_directive(object_name="sqlparse", profile_name="config_dev")

    # sql_text = _util_file_.identity_load_file(table_def_filepath)
    sql_text = _util_file_.json_load(table_def_filepath)
    column_with_desc = []

    from _knowledge_base import _knowledge_base_comment

    kb_comment_inst = _knowledge_base_comment.KnowledgeBaseComment()
    kb_comment_inst.load()


    for each_column in object_directive.extract_info_from_ddl(sql_text):

        comment = kb_comment_inst.query(each_column[0])
        if comment:
            column_with_desc.append((each_column[0], each_column[1], comment))
        else:
            kb_comment_inst.add(each_column[0], each_column[0].replace("_", " "))
            column_with_desc.append((each_column[0], each_column[1], each_column[0].replace("_", " ")))

    kb_comment_inst.save()

    print(column_with_desc)

    object_directive.generate_tubibricks_manifest_comment(
        table_name=model_name,
        table_description=model_description,
        manifest_filepath=manifest_filepath,
        output_filepath=output_filepath,
        column_names=column_with_desc,
        column_key=["_id"],
        not_null_columns=["_id", "ds"]
    )
    _common_.info_logger(f"end time:{datetime.now()}", logger=logger)
    return True


if __name__ == '__main__':
    gen_tubibricks_manifest()