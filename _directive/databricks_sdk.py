from inspect import currentframe
from typing import List, Dict, Tuple, Iterator, Union

from botocore.waiter import Waiter
from databricks.sdk.service.catalog import CatalogInfo
from wirerope.callable import Callable

from _meta import _meta as _meta_
from _common import _common as _common_
from _config import config as _config_
from logging import Logger as Log
from databricks.sdk.service.jobs import BaseJob, Wait
from _util import _util_file as _util_file_
from os import path
from task import task_completion
from databricks import sdk
from databricks.sdk.service import catalog


"""

poetry add databricks-sdk


https://github.com/databricks/databricks-sdk-py/blob/main/databricks/sdk/service/jobs.py

class JobsAPI:
        The Jobs API allows you to create, edit, and delete jobs.

"""
from databricks.sdk import WorkspaceClient

class DirectiveDatabricks_SDK(metaclass=_meta_.MetaDirective):
    def __init__(self,
                 profile_name: str,
                 config: _config_.ConfigSingleton = None,
                 logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton()

        _common_.info_logger(self._config.config.get("DATABRICKS_HOST"), logger=logger)
        _common_.info_logger(self._config.config.get("DATABRICKS_TOKEN"), logger=logger)
        self.client = WorkspaceClient(host=self._config.config.get("DATABRICKS_HOST"),
                            token=self._config.config.get("DATABRICKS_TOKEN")
                            )

    @_common_.exception_handler
    def list_run_active(self,
                        user_name: str = "",
                        logger: Log = None,
                        *arg,
                        **kwargs) -> List[Tuple[any, any, any, any]]:
        """ list all active databricks job

        Args:
            user_name: optional parameters if specified then jobs only belong to that user otherwise all users
            logger: logger object
            *arg:
            **kwargs:

        Returns:
            return a list associated fields of active databricks jobs

        """

        return [(each_job.cluster_instance, each_job.job_id, each_job.run_id, each_job.status.state.value) for each_job in
                self.client.jobs.list_runs(active_only=True) if user_name == "" or each_job.creator_user_name == user_name]

    @_common_.exception_handler
    def list_runs_by_jobid(self, job_id: int, logger: Log = None, *arg,**kwargs, ) -> List[Tuple[any, any, any, any]]:
        """ list all active databricks job run by job_id

        Args:
            job_id: databricks workflow job id
            logger: logger object
            *arg:
            **kwargs:

        Returns:
            return a list job ids of active databricks jobs

        """
        yield from self.client.jobs.list_runs(job_id=job_id)


    @_common_.exception_handler
    def list_catalog(self,
                     logger: Log = None,
                     *arg,
                     **kwargs) -> Iterator[CatalogInfo]:
        """ list the databricks catalog

        Args:
            logger: logger object
            *arg:
            **kwargs:

        Returns:
            return a list catalogs

        """
        return self.client.catalogs.list()


    @_common_.exception_handler
    def get_job_detail_by_id(self, job_id, logger: Log = None,*arg, **kwargs) -> Iterator[BaseJob]:
        """ list the databricks jobs by job_id

        Args:
             job_id: databricks workflow job id
            logger: logger object
            *arg:
            **kwargs:

        Returns:
            yield a list catalogs

        """
        yield from self.client.jobs.get(job_id)


    @_common_.exception_handler
    def get_job_id_by_name(self, job_name: str, logger: Log = None, *arg, **kwargs) -> Iterator[BaseJob]:
        """ list the databricks jobs by job_id

        Args:
             job_name: databricks workflow job name
             logger: logger object
             *arg:
             **kwargs:

        Returns:
            yield a list catalogs

        """
        print([each_job.job_id for each_job in self.client.jobs.list(name=job_name)])
        return [each_job.job_id for each_job in self.client.jobs.list(name=job_name)]

    @_common_.exception_handler
    def job_run_now_job_id(self, job_id: int, logger: Log = None, *arg, **kwargs) -> Wait:
        """ run the databricks workflow by job_id

        Args:
             job_id: databricks workflow job id
             logger: logger object
             *arg:
             **kwargs:

        Returns:
            yield a databricks wait object

        """
        return self.client.jobs.run_now(job_id)

    def job_repair_now_job_id(self, job_run_id: int, logger: Log = None, *arg, **kwargs) -> Tuple[bool, Union[str, None]]:
        """ restart workflow by job_run_id

        Args:
             job_run_id: databricks workflow job run id
             logger: logger object
             *arg:
             **kwargs:

        Returns:
            yield a databricks wait object

        """
        try:
            self.client.jobs.repair_run(run_id=job_run_id, rerun_all_failed_tasks=True)
            return True, None
        except Exception as err:
            _common_.error_logger(currentframe().f_code.co_name,
                err,
                logger=None,
                mode="error",
                ignore_flag=True)
            return False, err

    @_common_.exception_handler
    def get_job_run_id(self, job_id: int, logger: Log = None, *arg, **kwargs) -> List:
        """ restart workflow by job_run_id

        Args:
             job_id: databricks workflow job run id
             logger: logger object
             *arg:
             **kwargs:

        Returns:
            yield a list of running jobs

        """
        yield from [(each_job.job_id, each_job.run_id, each_job.original_attempt_run_id, each_job.state.life_cycle_state.value)
                    for each_job in self.list_runs_by_jobid(job_id=job_id) if each_job.state.life_cycle_state.value == "RUNNING"]

    @_common_.exception_handler
    def run_monitor_job(self,
                        user_name: str,
                        job_name: str,
                        logger: Log = None, *arg, **kwargs) -> bool:
        """ monitor the job by job name and restart it if it fails

        Args:
            user_name: user name of the databricks workflow jobs
            job_name: databricks workflow job name
            logger: logger object
            *arg:
            **kwargs:

        Returns:
            return True if it completes successfully otherwise return False

        "[dev jian_huang] revenue_bydevice_daily"
        """
        from time import sleep
        _WAIT_TIME_INTERVAL_ = 60

        @_common_.exception_handler
        def get_last_running_id(job_id: int) -> Tuple[int, int, int, Union[str, None], Union[int, None]]:
            try:
                if runs := list(self.list_runs_by_jobid(job_id=job_id)):
                    return sorted([(getattr(each_job, "job_id"),
                                    getattr(each_job, "run_id"),
                                    getattr(each_job, "original_attempt_run_id"),
                                    getattr(getattr(getattr(each_job, "state"), "life_cycle_state"), "value"),
                                    getattr(each_job, "start_time")) for each_job in runs],  key=lambda x: - x[4])
                else:
                    return -1, -1, -1, None, None

            except Exception as err:
                _common_.error_logger(currentframe().f_code.co_name,
                      f"there are multiple jobs found with the same name, please check",
                      logger=logger,
                      mode="error",
                      ignore_flag=True)
                return -1, -1, -1, None, None

        @_common_.exception_handler
        def monitoring_job(user_name: str,
                           job_id: int):
            while running_job := get_last_running_id(job_id=job_id):
                job_id, job_run_id, job_orginal_id, job_status, start_time = running_job[0]
                print(job_run_id, job_status)
                if job_status == "RUNNING":
                    _common_.info_logger(f"job_id {job_id} is running, please wait...", logger=logger)
                    sleep(_WAIT_TIME_INTERVAL_)
                elif job_status == "INTERNAL_ERROR":
                    _common_.info_logger(f"job_id {job_id} is encountered internal error, starting retry...", logger=logger)
                    return_code, error_msg = self.job_repair_now_job_id(job_run_id=job_run_id)
                    if return_code is False:
                        _common_.error_logger(currentframe().f_code.co_name,
                            f"Error, cannot restart the job due to {error_msg}",
                            logger=logger,
                            mode="error",
                            ignore_flag=False)

                else:
                    _common_.info_logger(f"job_id {job_id} is {status}, exit waiting.", logger=logger)
                    break

        print(job_name)
        jobs = list(self.get_job_id_by_name(job_name=job_name))
        print(jobs)

        if len(jobs) > 1:
            _common_.error_logger(currentframe().f_code.co_name,
                                  f"there are multiple jobs found with the same name, please check",
                                  logger=logger,
                                  mode="error",
                                  ignore_flag=False)

        if len(jobs) == 0:
            job_id, job_run_id, job_orginal_id, status, start_time  = -1, -1, -1, None, None
        else:
            job_id, job_run_id, job_orginal_id, status, start_time = get_last_running_id(jobs[0])[0]

        if job_run_id == -1:
            self.job_run_now_job_id(job_id=jobs[0])
            sleep(_WAIT_TIME_INTERVAL_)
        elif status == "INTERNAL_ERROR":
            try:
                _common_.info_logger(f"job_id {jobs[0]} is encountered internal error, starting retry...",
                                     logger=logger)
                return_code, error_msg = self.job_repair_now_job_id(job_run_id=job_run_id)
                if return_code is False and "Number of tasks changed" in str(error_msg):
                    self.job_run_now_job_id(job_id=jobs[0])

            except Exception as err:
                _common_.error_logger(currentframe().f_code.co_name,
                      f"Error, {err}",
                      logger=logger,
                      mode="error",
                      ignore_flag=False)

                sleep(_WAIT_TIME_INTERVAL_)

        monitoring_job(f"{user_name}@tubi.tv", jobs[0])
        return True








    @_common_.exception_handler
    def list_runs(self,
                 user_name: str = "",
                 *arg,
                 **kwargs) -> List[Tuple[any, any, any, any]]:

        return [(each_job.cluster_instance, each_job.job_id, each_job.run_id, each_job.status.state.value) for each_job in
                self.client.jobs.list_runs() if user_name == "" or each_job.creator_user_name == user_name]








