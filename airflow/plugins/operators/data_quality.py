from datetime import datetime, timedelta

from airflow.hooks import S3_hook
from typing import Optional

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    query_format = "date > '{start}' and date < '{end}' "
    ui_color = '#e67e22'

    @apply_defaults
    def __init__(self,
                 conn_id: str = "",
                 aws_con: str = "",
                 aws_bucket_name: str = "",
                 tables: Optional[dict] = None,
                 tables_with_rows: Optional[dict] = None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        if tables is None:
            tables = dict()
        self.conn_id = conn_id
        self.tables = tables
        self.tables_with_rows = tables_with_rows
        self.aws_bucket_name = aws_bucket_name
        self.aws_con = aws_con

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)

        self.start = datetime.strptime(context["ds"], "%Y-%m-%d")
        self.end = datetime.strptime(context["ds"], "%Y-%m-%d") + timedelta(days=1)
        filter_query = self.query_format.format(start=self.start.strftime("%Y-%m-%d"), end=self.end.strftime("%Y-%m-%d"))
        self.log.info(f"Filter query {filter_query}")
        remote_providers = ["bisq", "paxful"]
        total = 0
        for provider in remote_providers:
            filename = f"{provider}({context['ds']}).json"
            hook = S3_hook.S3Hook(self.aws_con)
            total += hook.read_key(filename, self.aws_bucket_name).count('\n')

        failted_tests = []
        for table in self.tables:
            self.log.info(f"Starting data quality on table with total : {table}")
            records = redshift_hook.get_records(f"SELECT count(*) FROM {table} where {filter_query} ;")

            if len(records) < 1 or records[0][0] != total:
                self.log.error(f"Data quality failed for table : {table}. count {records[0][0]}, total file:{total}")
                failted_tests.append(f"SELECT count(*) FROM {table};")
            else:
                self.log.info(f"Data quality Passed on table : {table}!!!")

        for table in self.tables_with_rows:
            self.log.info(f"Starting data quality on table : {table}")
            records = redshift_hook.get_records(f"SELECT count(*) FROM {table} where {filter_query};")

            if len(records) < 1 or records[0][0] < 1:
                self.log.error(f"Data quality failed for table : {table}. count {records[0][0]}, total file:{total}")
                failted_tests.append(f"SELECT count(*) FROM {table};")
            else:
                self.log.info(f"Data quality Passed on table : {table}!!!")

        if len(failted_tests) > 0:
            self.log.info(failted_tests)
            raise ValueError('Data quality check failed')
        self.log.info(f"Data quality done")
