from datetime import datetime, timedelta

from typing import Union, Optional

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadTableOperator(BaseOperator):
    ui_color = '#80BD9E'
    query_format = "{date_column} > {start} and {date_column} < {end} "

    @apply_defaults
    def __init__(self,
                 conn_id: str = "",
                 sql_query: Union[str, list] = "",
                 empty_table: bool = False,
                 table_name: str = "",
                 *args, **kwargs):
        super(LoadTableOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql_query = sql_query
        self.table_name = table_name
        self.empty_table = empty_table

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)
        self.start = int(datetime.strptime(context["ds"], "%Y-%m-%d").timestamp())
        self.end = int((datetime.strptime(context["ds"], "%Y-%m-%d") + timedelta(days=1)).timestamp())

        self.log.info(f"Filter by")
        filter_bisq = self.query_format.format(date_column="trade_date", start=self.start * 1000, end=self.end * 1000)
        filter_paxful = self.query_format.format(date_column="date", start=self.start, end=self.end)

        if type(self.sql_query) == str:
            self.sql_query = [self.sql_query]

        queries = [
            q.replace('[filter_bisq]', filter_bisq).replace('[filter_paxful]', filter_paxful)
            if '[filter' in q else q for q in self.sql_query]
        self.log.info("Queries to run")
        self.log.info(queries)
        if self.empty_table:
            self.log.info(f"Empty table {self.table_name}")
            queries.insert(0, f"TRUNCATE TABLE {self.table_name};")

        self.log.info(f"Running to load the dimension Table {self.table_name}")
        redshift_hook.run(queries)
        self.log.info(f"Dimension Table {self.table_name} ready!")
