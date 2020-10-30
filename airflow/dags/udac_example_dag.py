from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators import ( CreateTableOperator, FetchApiOperator)
from airflow.operators.dummy_operator import DummyOperator

from helpers import SqlQueries

s3_bucket = 'udacity-dend'
song_s3_key = "song_data"
log_s3_key = "log_data"
log_json_file = "log_json_path.json"

default_args = {
    'owner': 'udacity',
    'depends_on_past': True,
    'start_date': datetime(2020, 10, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': True
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
          )

fetch_api = FetchApiOperator(task_id="fetch_api", dag=dag,aws_con="aws_con",
        aws_key="test.json",
        aws_bucket_name="dataengineer-udacity")
create_table = CreateTableOperator(task_id="Create_table", dag=dag, conn_id="redshift")



start_operator = DummyOperator(task_id='Begin_execution', dag=dag)


start_operator >> fetch_api >> create_table

