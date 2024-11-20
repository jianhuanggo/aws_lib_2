from heapq import heappush
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
        yield from (each_job.job_id for each_job in self.client.jobs.list(name=job_name))

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

    @_common_.exception_handler
    def job_repair_now_job_id(self, job_run_id: int, logger: Log = None, *arg, **kwargs) -> Wait:
        """ restart workflow by job_run_id

        Args:
             job_run_id: databricks workflow job run id
             logger: logger object
             *arg:
             **kwargs:

        Returns:
            yield a databricks wait object

        """
        # print(job_run_id)
        # exit(0)
        return self.client.jobs.repair_run(run_id=job_run_id, rerun_all_failed_tasks=True)

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
    def run_monitor_job(self, job_name: str, logger: Log = None, *arg, **kwargs) -> bool:
        """ monitor the job by job name and restart it if it fails

        Args:
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

        def get_last_running_id(job_id: int) -> Tuple[int, Union[int, None]]:
            # get the latest run id by start time, not the end time
            print(sorted([(getattr(each_job, "job_id"),
                                         getattr(each_job, "run_id"),
                                         getattr(each_job, "original_attempt_run_id"),
                                         getattr(getattr(getattr(each_job, "state"), "life_cycle_state"), "value"),
                                         getattr(each_job, "end_time")) for each_job in
                                        self.list_runs_by_jobid(job_id=job_id)]))

            try:
                return (runs := sorted([(getattr(each_job, "job_id"),
                                         getattr(each_job, "run_id"),
                                         getattr(each_job, "original_attempt_run_id"),
                                         getattr(getattr(getattr(each_job, "state"), "life_cycle_state"), "value"),
                                         getattr(each_job, "start_time")) for each_job in
                                        self.list_runs_by_jobid(job_id=job_id)],
                                       key=lambda x: - x[4])) and (runs[0][1], runs[0][3])

            except Exception as err:
                print(err)
                return -1, None

        def monitoring_job(user_name: str,
                           job_id: int):

            while running_job := get_last_running_id(job_id=job_id):
                job_run_id, job_status = running_job
                print(job_run_id, job_status)
                if job_status == "RUNNING":
                    _common_.info_logger(f"job_id {job_id} is running, please wait...", logger=logger)
                    sleep(_WAIT_TIME_INTERVAL_)
                elif job_status == "INTERNAL_ERROR":
                    _common_.info_logger(f"job_id {job_id} is encountered internal error, starting retry...", logger=logger)
                    self.job_repair_now_job_id(job_run_id=job_run_id)
                else:
                    _common_.info_logger(f"job_id {job_id} is {status}, exit waiting.", logger=logger)
                    break


        jobs = list(self.get_job_id_by_name(job_name=job_name))

        job_run_id, status = get_last_running_id(jobs[0])
        # print(job_run_id, status)
        # exit(0)
        if len(jobs) > 1:
            _common_.error_logger(currentframe().f_code.co_name,
                                  f"there are multiple jobs found with the same name, please check",
                                  logger=None,
                                  mode="error",
                                  ignore_flag=False)

        if job_run_id == -1:
            self.job_run_now_job_id(job_id=jobs[0])
            sleep(_WAIT_TIME_INTERVAL_)
        elif status == "INTERNAL_ERROR":
            _common_.info_logger(f"job_id {jobs[0]} is encountered internal error, starting retry...",
                                 logger=logger)
            self.job_repair_now_job_id(job_run_id=job_run_id)
            sleep(_WAIT_TIME_INTERVAL_)





        # status = list(self.list_runs_by_jobid(job_id=jobs[0]))

        #
        # if len(list(self.list_runs_by_jobid(job_id=jobs[0]))) == 0:
        #     self.job_run_now_job_id(job_id=jobs[0])
        #     sleep(_WAIT_TIME_INTERVAL_)
        # else:
        #     last_run = sorted([(each_job.job_id,
        #                         each_job.run_id,
        #                         each_job.original_attempt_run_id,
        #                         each_job.state.life_cycle_state.value,
        #                         each_job.end_time) for each_job in status], key=lambda x: - x[4])[0]
        #     if last_run[3] == "INTERNAL_ERROR":
        #         _common_.info_logger(f"job_id {last_run[0]} is encountered internal error, starting retry...", logger=logger)
        #         _common_.info_logger(self.job_repair_now_job_id(job_run_id=last_run[1]), logger=logger)
        #         sleep(_WAIT_TIME_INTERVAL_)

        monitoring_job("jian.huang@tubi.tv", jobs[0])
        return True








    @_common_.exception_handler
    def list_runs(self,
                 user_name: str = "",
                 *arg,
                 **kwargs) -> List[Tuple[any, any, any, any]]:

        return [(each_job.cluster_instance, each_job.job_id, each_job.run_id, each_job.status.state.value) for each_job in
                self.client.jobs.list_runs() if user_name == "" or each_job.creator_user_name == user_name]








