
import pandas as pd
from logging import Logger as Log

from dns.e164 import query

from _meta import _meta as _meta_
from _config import config as _config_
from _common import _common as _common_
from databricks.connect import DatabricksSession


class APIDatabricksCluster(metaclass=_meta_.MetaAPI):

    def __init__(self,
                 profile_name: str,
                 config: _config_.ConfigSingleton = None,
                 logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton()

        self.client = DatabricksSession.builder.remote(
            host=self._config.config.get("DATABRICKS_HOST"),
            token=self._config.config.get("DATABRICKS_TOKEN"),
            cluster_id=self._config.config.get("DATABRICKS_CLUSTER_ID")
        ).getOrCreate()


    def query(self, query_string: str, logger: Log = None) -> pd.DataFrame:
        """ execute sql in the databricks compute class and return the query result

        Args:
            query_string: query string
            logger: logger object

        Returns: query result in the pandas dataframe
        """
        from datetime import datetime
        start_time = datetime.now()
        sql_reformat = query_string[:20].replace("\n", "")
        _common_.info_logger(f"starting query {sql_reformat} at {start_time}", logger=logger)

        query_string = """
        select ds, count(1) as cnt
        from hive_metastore.tubidw.revenue_bydevice_daily where ds between 
        '2024-08-01' and '2024-08-02'
        group by 1
        """
        x = self.client.sql(query_string)
        x.show()
        exit(0)
        result = self.client.sql(query_string).toPandas()
        end_time = datetime.now()
        _common_.info_logger(f"query completed at {end_time}", logger=logger)
        _common_.info_logger(f"total duration is {end_time - start_time}", logger=logger)
        return result
