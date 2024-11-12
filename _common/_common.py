import functools
import os.path
import time
import inspect
from sys import exit
from functools import wraps
from _util import _util_file
from logging import Logger as Log
from typing import TypeVar, Callable, Any
from _common import _common as _common_


RT = TypeVar("RT")


def error_logger(func_str: str,
                 error,
                 logger: Log = None,
                 addition_msg: str = "",
                 mode: str = "critical",
                 ignore_flag: bool = True,
                 set_trace: bool = False) -> None:
    """Display error message in a logger if there is one otherwise stdout
    Args:
        func_str: calling function, so error msg can be associated correctly
        error: exception captured
        logger: Whether error msg should be persisted in a log file
        addition_msg: A set of parameters which need to be verified
        mode: error mode, either critical, debug, error or info
        ignore_flag: It will return to the calling function if set to True otherwise program will terminate
        set_trace: This will log stack trace
    Returns:
        No return value.
    """
    def _not_found(*args, **kwargs):
        raise "error mode should be 'critical', 'debug', 'error' and 'info'"
    if logger:
        _log_mode = {"critical": logger.critical,
                     "debug": logger.debug,
                     "error": logger.error,
                     "info": logger.info}
    try:
        _log_mode.get(mode, _not_found)(f"Error in {func_str}! {addition_msg} {error}") if logger \
            else print(f"Error in {func_str}! {addition_msg} {error}")
        if logger and set_trace:
            logger.exception("trace")
        return exit(99) if not ignore_flag else None
    except Exception as err:
        raise err


def info_logger(message: str = "",
                func_str: str = "",
                logger: Log = None,
                addition_msg: str = ""
                ) -> None:
    """Display message in a logger if there is one otherwise stdout
    Args:
        message: display message
        func_str: calling function, so error msg can be associated correctly
        logger: Whether error msg should be persisted in a log file
        addition_msg: A set of parameters which need to be verified
    Returns:
        No return value.
    """
    try:
        if func_str:
            message = f"{func_str}: {message}"
        logger.info(f"{message} {addition_msg}") if logger else print(f"{message} {addition_msg}")

    except Exception as err:
        raise err


def retry(tries=3,
          delay=3,
          backoff=2,
          silent=False,
          validation_func: Callable = None,
          logger=None) -> Callable[[Callable[..., RT]], Callable[..., RT]]:
    """ A decorator to implement retry functionality

    Args:
        tries: number of retries
        delay: delay between retries
        backoff: backoff multiplier
        silent: notification msg
        validation_func: custom validation function
        logger: Whether error msg should be persisted in a log file

    Returns: a function

    """

    def default_validation_func(result: Any) -> bool:
        return True if result is not None and result != "" else False

    def send_msg(is_slient: bool, msg: str, msg_logger: Log = None) -> None:
        if not is_slient:
            info_logger(msg, logger=msg_logger)

    def deco_retry(func):
        @wraps(func)
        def func_retry(*args, **kwargs):
            _delay = delay
            _sm = None
            _sm_parameter = None
            for _retry_num in range(tries):
                try:
                    _ret = func(*args, **kwargs)
                    _validation_func = default_validation_func if validation_func is None else validation_func
                    if _validation_func(_ret):
                        return _ret
                except Exception as err:
                    _sm_parameter = (silent, f"{str(err) if str(err) != '' else repr(err)}, "
                                             f"Retrying in {_delay} seconds...", logger)
                    _sm = send_msg
                _sm(*_sm_parameter) if _sm else send_msg(silent, f"Retrying in {_delay} seconds...")
                time.sleep(_delay)
                _delay *= backoff
        return func_retry
    return deco_retry

from pprint import pprint


def get_aws_resource(next_token_name: str):
    def get_resource(func):
        @wraps(func)
        def function(*args, **kwargs):
            try:
                _curr_token = ""
                _acc_result = []

                while True:
                    _parameter = {"next_t": {next_token_name: _curr_token}, "acc_result": _acc_result} if _curr_token else {}
                    _ret = func(*args, **{**kwargs, **_parameter})
                    _acc_result.extend(_ret.get("result"))
                    if not _ret.get("next_t"): break

                    _curr_token = _ret.get("next_t", 0)
                return _acc_result

            except Exception as err:
                error_logger(func.__name__,
                             err,
                             logger=kwargs.get("logger"),
                             mode="error",
                             ignore_flag=False)

        return function
    return get_resource


def exception_handler(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as err:
            error_logger(func.__name__,
                         err,
                         logger=kwargs.get("logger"),
                         mode="error",
                         ignore_flag=False)

    return wrapper


def cache_result(filepath: str):
    def wrapper(func):
        @functools.wraps(func)
        def function(*args, **kwargs):
            if os.path.isfile(filepath):
                _common_.info_logger(f"found {filepath}, loading cached result")
                return iutil_file.json_load(filepath)
            try:
                _ret = func(*args, **kwargs)
                _common_.info_logger(f"save result to {filepath}")
                iutil_file.json_dump(filepath, _ret)
                return _ret
            except Exception as err:
                error_logger(func.__name__,
                             err,
                             logger=kwargs.get("logger"),
                             mode="error",
                             ignore_flag=False)
        return function
    return wrapper


