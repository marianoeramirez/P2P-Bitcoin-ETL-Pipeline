from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateTableOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id: str = "",
                 *args, **kwargs):
        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)
        query = open("/home/workspace/airflow/create_tables.sql").read()

        redshift_hook.run(query)
        self.log.info(f"Create Table ready!")
