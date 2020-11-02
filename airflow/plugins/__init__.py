from airflow.plugins_manager import AirflowPlugin

from . import operators
from . import helpers


class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.FetchApiOperator,
        operators.DataQualityOperator,
        operators.StageToRedshiftOperator,
        operators.CreateTableOperator,
        operators.LoadTableOperator
    ]
    helpers = [
        helpers.SqlQueries
    ]
