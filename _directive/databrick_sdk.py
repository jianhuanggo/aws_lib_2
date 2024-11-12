from typing import List, Dict, Tuple, Iterator
from _meta import _meta as _meta_
from _common import _common as _common_
from _config import config as _config_
from logging import Logger as Log
from databricks.sdk.service.jobs import BaseJob
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


class DirectiveDatabrick_SDK(metaclass=_meta_.MetaDirective):
    def __init__(self,
                 profile_name: str,
                 config: _config_.ConfigSingleton = None,
                 logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton()

        print(self._config.config.get("DATABRICKS_HOST"))
        print(self._config.config.get("DATABRICKS_TOKEN"))
        self.client = WorkspaceClient(host=self._config.config.get("DATABRICKS_HOST"),
                            token=self._config.config.get("DATABRICKS_TOKEN")
                            )
        # counter = 0
        # for item in w.jobs.list():
        #     print(item)
        #     if counter > 5:
        #         break
        #     counter += 1


    @_common_.exception_handler
    def list_run_active(self,
                        user_name: str = "",
                        *arg,
                        **kwargs) -> List[Tuple[any, any, any, any]]:
        """ list all active databricks job

        Args:
            user_name: optional parameters if specified then jobs only belong to that user otherwise all users
            *arg:
            **kwargs:

        Returns:
            return a list associated fields of active databricks jobs

        """

        return [(each_job.cluster_instance, each_job.job_id, each_job.run_id, each_job.status.state.value) for each_job in
                self.client.jobs.list_runs(active_only=True) if user_name == "" or each_job.creator_user_name == user_name]

    @_common_.exception_handler
    def list_runs_by_jobid(self, job_id: int, *arg,**kwargs) -> List[Tuple[any, any, any, any]]:
        yield from self.client.jobs.list_runs(job_id=job_id)


    @_common_.exception_handler
    def list_catalog(self,
                  *arg,
                  **kwargs) -> List[Tuple[any, any, any, any]]:


        from pprint import pprint
        for each_catelog in self.client.catalogs.list():
            print(each_catelog.name, each_catelog.catalog_type)
            # pprint(each_catelog.name, each_catelog.full_name)

        print(self.client.metastores.list())

    @_common_.exception_handler
    def get_job_detail_by_id(self, job_id, *arg, **kwargs) -> Iterator[BaseJob]:
        yield from self.client.jobs.get(job_id)


    @_common_.exception_handler
    def get_job_id_by_name(self, job_name: str, *arg, **kwargs) -> Iterator[BaseJob]:
        yield from (each_job.job_id for each_job in self.client.jobs.list(name=job_name))

    @_common_.exception_handler
    def job_run_now_job_id(self, job_id: int, *arg, **kwargs):
        return self.client.jobs.run_now(job_id)

    @_common_.exception_handler
    def job_repair_now_job_id(self, job_run_id: int, *arg, **kwargs):
        return self.client.jobs.repair_run(run_id=job_run_id)

    @_common_.exception_handler
    def get_job_run_id(self, job_id: int, *arg, **kwargs):
        return [(each_job.job_id,
               each_job.run_id,
               each_job.original_attempt_run_id,
               each_job.state.life_cycle_state.value,
               ) for each_job in self.list_runs_by_jobid(job_id=job_id) if each_job.state.life_cycle_state.value == "RUNNING"]











    @_common_.exception_handler
    def list_runs(self,
                 user_name: str = "",
                 *arg,
                 **kwargs) -> List[Tuple[any, any, any, any]]:

        return [(each_job.cluster_instance, each_job.job_id, each_job.run_id, each_job.status.state.value) for each_job in
                self.client.jobs.list_runs() if user_name == "" or each_job.creator_user_name == user_name]








