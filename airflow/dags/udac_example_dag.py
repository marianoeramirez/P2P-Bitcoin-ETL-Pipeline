from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators import ( CreateTableOperator)
from airflow.operators.dummy_operator import DummyOperator

from helpers import SqlQueries

s3_bucket = 'udacity-dend'
song_s3_key = "song_data"
log_s3_key = "log_data"
log_json_file = "log_json_path.json"

default_args = {
    'owner': 'udacity',
    'depends_on_past': True,
    'start_date': datetime(2021, 1, 12),
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

create_table = CreateTableOperator(task_id="Create_table", dag=dag, conn_id="redshift")



start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_table >> start_operator
