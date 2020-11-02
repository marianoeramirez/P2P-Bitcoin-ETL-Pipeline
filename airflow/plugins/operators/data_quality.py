from typing import Optional

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#e67e22'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "",
                 tables: Optional[dict] = None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        if tables is None:
            tables = dict()
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        failted_tests = []
        for table in self.tables:
            self.log.info(f"Starting data quality on table : {table}")
            records = redshift_hook.get_records(f"SELECT count(*) FROM {table};")

            if len(records) < 1 or records[0][0] != self.tables[table]:
                self.log.error(f"Data quality failed for table : {table}. count {records[0][0]}")
                failted_tests.append(f"SELECT count(*) FROM {table};")
            else:
                self.log.info(f"Data quality Passed on table : {table}!!!")

        if len(failted_tests) > 0:
            self.log.info(failted_tests)
            raise ValueError('Data quality check failed')
        self.log.info(f"Data quality done")

