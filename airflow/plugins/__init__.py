from airflow.plugins_manager import AirflowPlugin

from . import operators
from . import helpers


class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.DataQualityOperator,
        operators.CreateTableOperator
    ]
    helpers = [
        helpers.SqlQueries
    ]
