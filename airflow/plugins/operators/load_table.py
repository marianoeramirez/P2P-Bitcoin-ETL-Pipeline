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
                 date_column: Optional[str] = None,
                 table_name: str = "",
                 *args, **kwargs):
        super(LoadTableOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql_query = sql_query
        self.table_name = table_name
        self.empty_table = empty_table
        self.date_column = date_column

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)
        self.start = int(datetime.strptime(context["ds"], "%Y-%m-%d").timestamp())
        self.end = int((datetime.strptime(context["ds"], "%Y-%m-%d") + timedelta(days=1)).timestamp())

        if self.date_column:
            self.log.info(f"Filter by {self.date_column}")
            filter_query = self.query_format.format(
                {"date_column": self.date_column, "start": self.start, "end": self.end})

            queries = [q.replace('[filter]', filter_query) if '[filter]' in q else q for q in self.sql_query]
        else:
            queries = self.sql_query

        if self.empty_table:
            self.log.info(f"Empty table {self.table_name}")
            queries.insert(0, f"TRUNCATE TABLE {self.table_name};")

        self.log.info(f"Running to load the dimension Table {self.table_name}")
        redshift_hook.run(queries)
        self.log.info(f"Dimension Table {self.table_name} ready!")
