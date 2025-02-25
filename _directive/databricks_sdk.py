import json
from datetime import time
from inspect import currentframe
from typing import List, Dict, Tuple, Iterator, Union
from venv import logger
import re
from base64 import b64decode, b64encode
from botocore.waiter import Waiter
from databricks.sdk.service.catalog import CatalogInfo
from databricks.sdk.service.workspace import ExportFormat, ImportFormat
from faiss.contrib.datasets import username
from matplotlib.font_manager import json_dump
from wirerope.callable import Callable

from datetime import datetime
from _meta import _meta as _meta_
from _common import _common as _common_
from _config import config as _config_
from logging import Logger as Log
from databricks.sdk.service.jobs import BaseJob, Wait, SparkPythonTask, SubmitTask, RunNow, NotebookTask
from _util import _util_file as _util_file_
from os import path
from task import task_completion
from databricks import sdk
from databricks.sdk.service import catalog



__TIME_WAIT__ = 30

"""

poetry add databricks-sdk


https://github.com/databricks/databricks-sdk-py/blob/main/databricks/sdk/service/jobs.py

class JobsAPI:
        The Jobs API allows you to create, edit, and delete jobs.

"""
from databricks.sdk import WorkspaceClient
from databricks.sdk import BillableUsageAPI

class DirectiveDatabricks_SDK(metaclass=_meta_.MetaDirective):
    def __init__(self,
                 profile_name: str,
                 config: _config_.ConfigSingleton = None,
                 logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton(profile_name=profile_name)

        _common_.info_logger(self._config.config.get("DATABRICKS_HOST"), logger=logger)
        _common_.info_logger(self._config.config.get("DATABRICKS_TOKEN"), logger=logger)
        self.client = WorkspaceClient(host=self._config.config.get("DATABRICKS_HOST"),
                            token=self._config.config.get("DATABRICKS_TOKEN")
                            )
        # print(self.client.api_client.account_id)

        # print(dir(self.client.api_client))
        # exit(0)

    # @_common_.exception_handler
    # def get_job_cluster(self) -> list | None:
    #     """return a list of databricks cluster
    #
    #     Returns: a list of clusters if any otherwise None
    #
    #     """
    #     from pprint import pprint
    #     cluster_name = self._config.config.get("") or self._config.config.get("")
    #     for each_cluster in self.list_clusters():
    #         pprint(each_cluster)
    #         exit(0)
    #         if each_cluster["name"] == cluster_name:
    #             pprint(each_cluster)
    #             return each_cluster

    @_common_.exception_handler
    def list_clusters(self) -> list | None:
        """return a list of databricks cluster

        Returns: a list of clusters if any otherwise None

        """
        return [each_cluster for each_cluster in self.client.clusters.list()]

    @_common_.exception_handler
    def list_workspace_file(self, filepath):
        return self.client.workspace.list(filepath)

    @_common_.exception_handler
    def upload_workspace_file(self, from_filepath: str, to_filepath: str, overwrite: bool = False):
        print(from_filepath)
        with open(from_filepath, 'rb') as f_in:
            data = f_in.read()
            print(data)
            return self.client.workspace.upload(to_filepath, data, overwrite=overwrite)

    @_common_.exception_handler
    def job_run(self,
                cluster_id: str,
                filepath: str,
                job_parameters: dict
                ) -> bool:

        notebook_job = NotebookTask(notebook_path=filepath, base_parameters=job_parameters)
        import time
        run = self.client.jobs.submit(run_name=f'wf-test-run-job-name{time.time_ns()}',
                            tasks=[
                                SubmitTask(existing_cluster_id=cluster_id,
                                           # spark_python_task=spark_python_job,
                                           notebook_task=notebook_job,
                                           task_key=f'task-test-run-job-name{time.time_ns()}')
                            ]).result()
        print(run)
        return True

    @_common_.exception_handler
    def list_dbfs_file(self, filepath):
        return self.client.dbutils.fs.ls(filepath)

    @_common_.exception_handler
    def list_dbfs_file(self, filepath):
        pass



    def billing_download_by_period(self, start_period: str = "2025-01", end_period: str = "2024-12"):

        """

            def download(self,
                 start_month: str,
                 end_month: str,
                 *,

        Args:
            start_period:
            end_period:

        Returns:

        """
        billing = BillableUsageAPI(api_client=self.client.api_client)
        print(billing)
        print(dir(billing))

        print(billing.download(start_period, end_period))
        exit(0)



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

    @_common_.exception_handler
    def get_notebook_path_from_job_id(self, job_id: str) -> str:
        job_details = self.client.jobs.get(job_id)
        for each_task in job_details.settings.tasks:
            if hasattr(each_task, "notebook_task"):
                return each_task.notebook_task.notebook_path

    @_common_.exception_handler
    def get_job_id_from_workflow_name(self, workflow_name: str) -> str:
        workflow_list = list(self.client.jobs.list(name=workflow_name))
        return workflow_list[0].job_id if len(workflow_list) > 0 else ""

    @_common_.exception_handler
    def get_notebook_content_from_path(self, notebook_path: str) -> str:
        return b64decode(self.client.workspace.export(path=notebook_path).content).decode("utf-8")

    @_common_.exception_handler
    def get_notebook_content_replace(self, notebook_path: str, search_string: str, replace_string: str) -> bool:
        """ edit the notebook content by search and replace

        Args:
            notebook_path: notebook path
            search_string: the string we are looking for
            replace_string: the string we are replacing

        # make sure there is an file extension for the notebook_path, w/o it, it would treat this as a directory
        Returns: return true if successful

        """

        notebook_content = self.client.workspace.export(path=notebook_path)
        notebook_content = b64decode(notebook_content.content)

        notebook_text = notebook_content.decode("utf-8")
        updated_notebook_text = notebook_text.replace(search_string, replace_string)

        if notebook_text == updated_notebook_text:
            _common_.info_logger(f"No occurrence of {search_string} found in the notebook", logger=logger)
            return False
        _common_.info_logger(f"replaced occurrences of {search_string} found in the notebook", logger=logger)
        notebook_content = self.client.workspace.upload(notebook_path,
            content=updated_notebook_text.encode("utf-8"),
            overwrite=True
        )
        _common_.info_logger(f"notebook of {notebook_path} updated successfully")
        return True

    @_common_.exception_handler
    def job_monitoring(self,
                       run_id: int,
                       max_timeout: int = 86400,
                       max_retries: int = 3) -> bool:

        """ edit the notebook content by search and replace

        Args:
            notebook_path: notebook path
            search_string: the string we are looking for
            replace_string: the string we are replacing

        # make sure there is an file extension for the notebook_path, w/o it, it would treat this as a directory
        Returns: return true if successful

        """
        start_time = datetime.timestamp(datetime.now())
        retries = 0
        from time import sleep
        sleep(3)
        print("AAAA")
        print(datetime.timestamp(datetime.now()) - start_time)

        while datetime.timestamp(datetime.now()) - start_time < max_timeout:
            try:
                run_status = self.client.jobs.get_run(run_id=run_id)

                # print(run_status)
                # exit(0)
                job_state = run_status.state
                _common_.info_logger(f"current status of job {run_id} is {job_state} ", logger=logger)

                if job_state.life_cycle_state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                    _common_.info_logger(f"run completed with final state: {job_state.result_state}", logger=logger)
                    return job_state.result_state


                sleep(__TIME_WAIT__)
            except Exception as err:
                retries += 1
                if retries > max_retries:
                    logger.error(f"Max retries exceeded: {max_retries}")
                    raise err
                sleep(__TIME_WAIT__)
                _common_.info_logger(f"encounter {err}, retrying...", logger=logger)
        _common_.info_logger(f"job run time out after {max_timeout}", logger=logger)

    @_common_.exception_handler
    def update_note_book(self,
                         workflow_name: str,
                         replace_string: str,
                         logger: Log = None):

        job_id = self.get_job_id_from_workflow_name(workflow_name)
        notebook_path = self.get_notebook_path_from_job_id(job_id)
        resource_content = self.get_notebook_content_from_path(notebook_path)
        regex_pattern = r"\d{4}-\d{2}-\d{2}"
        matches = re.findall(regex_pattern, resource_content)
        if len(set(matches)) > 1:
            _common_.error_logger(currentframe().f_code.co_name,
                             f"too many dates in the notebook, expecting 1 and getting {len(set(matches))}",
                             logger=logger,
                             mode="error",
                             ignore_flag=False)
        _common_.info_logger(f"replacing date {matches[0]} with {replace_string}", logger=logger)
        self.get_notebook_content_replace(notebook_path=notebook_path,
                                          search_string=matches[0],
                                          replace_string=replace_string)

    @_common_.cache_result("my_job_id.json")
    def get_jobs_by_username(self,
                             username: str = "",
                             logger: Log = None):
        jobs = self.client.jobs.list()
        user_jobs = []
        for job in jobs:
            if job.creator_user_name == username:
                user_jobs.append({"username": job.creator_user_name,
                                  "job_id": job.job_id,
                                  "effective_budget_policy_id": job.effective_budget_policy_id})
        return user_jobs














