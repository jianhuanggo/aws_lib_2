import os
import inspect
import sys
from collections import deque
from typing import List

from _util import _util_file as _util_file_
from typing import Callable

#
# @click.command()
# @click.option('--profile_name', required=False, type=str)
# @click.option('--host', required=False, type=str)
# @click.option('--token', required=False, type=str)
# @click.option('--cluster_id', required=False, type=str)
# @click.option('--time_interval', required=True, type=int)
# def database_keep_alive(profile_name: str,
#                         host: str,
#                         token: str,
#                         cluster_id: str,
#                         time_interval: int,
#                         logger: Log = None):
#
#     """ this script monitors databricks workflow job and restarts if necessary
#
#     Args:
#
#
#         host: database host
#         cluster_id: database cluster id
#         token: databricks token
#         time_interval: keep alive interval in seconds
#         profile_name: profile, contains environment variables regarding to databricks environment (config_dev, config_prod, config_stage)
#         logger: logging object
#
#     Returns:
#         return true if successful otherwise return false
#
#     """
#
#     _config = _config_.ConfigSingleton(profile_name=profile_name)
#
#     if profile_name:
#         _config.config["PROFILE_NAME"] = profile_name
#     elif "PROFILE_NAME" in os.environ:
#         _config.config["PROFILE_NAME"] = os.environ.get("PROFILE_NAME")

def get_original_func_filepath(func):
    while hasattr(func, '__wrapped__'):
        func = func.__wrapped__
    return inspect.getfile(func)

def get_relative_path_diff(parent_path: str, child_path: str) -> str:
    return os.path.relpath(child_path, parent_path)

def get_path_component(path: str) -> List:
    result = deque()
    while True:
        path, tail = os.path.split(path)
        if tail:
            result.appendleft(tail)
        else:
            if path:
                result.appendleft(path)
            break
    return list(result)

def get_file_basename(filename: str) -> str:
    from pathlib import Path
    return Path(filename).stem

def convert_flag(write_flg: bool = False, output_filepath: str = "") -> Callable:
    python_root = os.environ.get('PYTHONPATH', '') or sys.prefix
    def convert(func):
        func_name = func.__name__
        signature = inspect.signature(func)
        parameters = signature.parameters
        func_filepath = get_original_func_filepath(func)
        print(func_filepath)
        module_rel_path = get_relative_path_diff(parent_path=python_root, child_path=func_filepath)
        path_to_root = get_path_component(module_rel_path)
        if len(path_to_root) > 0: path_to_root[-1] = get_file_basename(path_to_root[-1])

        print(path_to_root)


        def sub_func(*args, **kwargs):
            options = []
            func_parameters = []
            for parameter_name, parameter in parameters.items():
                parameter_type = parameter.annotation if parameter.annotation is not inspect.Parameter.empty else ""
                required = parameter.default is inspect.Parameter.empty
                default = parameter.default if parameter.default is not inspect.Parameter.empty else None

                # @click.option('--profile_name', required=False, type=str)
                default_value = f", default = str" if default is None else ""
                options.append(f"@click.option('--{parameter_name}', required={required}, type={parameter_type.__name__})")
                func_parameters.append(parameter_name)

            string=f"""
import click
from datetime import datetime
from time import sleep
from logging import Logger as Log
from inspect import currentframe
from _common import _common as _common_
from _config import config as _config_
from _connect import _connect as _connect_


@click.command()
{chr(10).join(options)}
def run_{func_name}({", ".join(func_parameters)}, logger: Log = None):
    
    from {'.'.join(path_to_root[1:])} import {func_name}
    
    output = {func_name}({", ".join(func_parameters)})
    if not output:
        _common_.error_logger(currentframe().f_code.co_name,
                     f"command line utility should return True but it doesn't",
                     logger=logger,
                     mode="error",
                     ignore_flag=False)


if __name__ == "__main__":
    run_{func_name}()
"""
            if write_flg:
                _util_file_.identity_write_file(output_filepath, string)
            else:
                print(string)
        return sub_func
    return convert



