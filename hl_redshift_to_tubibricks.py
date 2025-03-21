
import click
from datetime import datetime
from time import sleep
from logging import Logger as Log
from inspect import currentframe
from _common import _common as _common_
from _config import config as _config_
from _connect import _connect as _connect_


@click.command()
@click.option('--profile_name', required=True, type=str)
@click.option('--database_name', required=True, type=str)
@click.option('--table_name', required=True, type=str)
@click.option('--cluster_id', required=True, type=str)
def run_hl_redshift_to_tubibricks(profile_name, database_name, table_name, cluster_id, logger: Log = None):
    """
    to start a sample run, you can use following command:

    python hl_redshift_to_tubibricks.py  --profile_name config_prod --database_name tubidw --table_name retention_sketch_daily_byplatform_bycountry --cluster_id 0320-172648-4s9hf5og
    """


    from _project.hl_redshift_to_tubibricks import hl_redshift_to_tubibricks

    output = hl_redshift_to_tubibricks(profile_name, database_name, table_name, cluster_id)
    if not output:
        _common_.error_logger(currentframe().f_code.co_name,
                     f"command line utility should return True but it doesn't",
                     logger=logger,
                     mode="error",
                     ignore_flag=False)


if __name__ == "__main__":
    run_hl_redshift_to_tubibricks()
