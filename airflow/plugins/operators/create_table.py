from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateTableOperator(BaseOperator):
    ui_color = '#3498db'

    @apply_defaults
    def __init__(self,
                 conn_id: str = "",
                 sql_query: str = "",
                 *args, **kwargs):
        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql_query = sql_query

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)
        redshift_hook.run(self.sql_query)
        self.log.info(f"Create Table ready!")
