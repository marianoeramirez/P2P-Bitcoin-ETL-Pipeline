from airflow.plugins_manager import AirflowPlugin

from . import operators
from . import helpers


class ETLPlugin(AirflowPlugin):
    name = "etl_plugin"
    operators = [
        operators.FetchAPIOperator
    ]
    helpers = [
        helpers.SqlQueries
    ]
