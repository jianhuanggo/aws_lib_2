import os

import click
from logging import Logger as Log
from _common import _common as _common_
from _error_handling import _error_handling
from _config import config as _config_

@click.command()
@click.option('--pattern_template_filepath', required=True, type=str)
@click.option('--dw_home', required=True, type=str)
@click.option('--profile_name', required=True, type=str)
@click.option('--development_env', required=False, type=str)
@click.option('--dry_run', required=False, type=str)
def apply_pattern3(pattern_template_filepath: str,
                  dw_home: str,
                  profile_name: str = "default",
                  development_env: str = "",
                  dry_run: bool = False,
                  logger: Log = None):

    _common_.info_logger(f"pattern_template_filepath: {pattern_template_filepath}")
    _common_.info_logger(f"dw_home: {dw_home}")
    _common_.info_logger(f"profile_name: {profile_name}")
    _common_.info_logger(f"development_env: {development_env}")
    _common_.info_logger(f"dry_run: {dry_run}")

    error_handle = _error_handling.ErrorHandlingSingleton(profile_name=profile_name, error_handler="subprocess")

    from _engine._subprocess import ShellRunner
    from _engine._command_protocol import execute_command_from_dag

    from datetime import datetime

    _common_.info_logger(f"start time:{datetime.now()}")

    from _pattern_template._process_template import _process_template
    _config = _config_.ConfigSingleton(profile_name=profile_name)
    for var_name, var_value in os.environ.items():
        _config.config[var_name] = var_value

    if dw_home:
        _config.config["DW_HOME"] = dw_home
    elif "DW_HOME" in os.environ:
        _config.config["DW_HOME"] = os.environ.get("DW_HOME")

    if dw_home:
        _config.config["PROFILE_NAME"] = profile_name
    elif "PROFILE_NAME" in os.environ:
        _config.config["PROFILE_NAME"] = os.environ.get("PROFILE_NAME")

    if development_env:
        _config.config["DEPLOYMENT_ENV"] = development_env
    elif "DEPLOYMENT_ENV" in os.environ:
        _config.config["DEPLOYMENT_ENV"] = os.environ.get("DEPLOYMENT_ENV")

    if dry_run:
        _config.config["DRY_RUN"] = dry_run
    elif "DRY_RUN" in os.environ:
        _config.config["DRY_RUN"] = os.environ.get("DRY_RUN")

    print(_config.config.get("DEPLOYMENT_ENV"))
    print(_config.config.get("DRY_RUN"))

    # exit(0)
    print(_config.config)

    # print(f"{__file__} function {currentframe().f_code.co_name}")
    # print("!!!", _config.config)
    # exit(0)

    t_task = _process_template.process_template(config=_config, template_name=pattern_template_filepath)
    shell_runner = ShellRunner(profile_name=profile_name)
    execute_command_from_dag(shell_runner, t_task.tasks)

    _common_.info_logger(f"end time:{datetime.now()}")

if __name__ == '__main__':
    apply_pattern3()

