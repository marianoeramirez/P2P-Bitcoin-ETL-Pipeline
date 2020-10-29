from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators import DummyOperator, FetchAPIOperator

default_args = {
    'owner': 'mariano',
    'start_date': datetime(2020, 10, 1),
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': True,
}


dag = DAG('etl_extract_information',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
          )


start_task = DummyOperator(
    task_id='dummy_start'
)

fetch_to_S3_task = FetchAPIOperator(
    task_id='fetch_to_S3_task',
    aws_con="aws_con",
    aws_key="test.json",
    aws_bucket_name="dataengineer-udacity",

)

# Use arrows to set dependencies between tasks
start_task >> fetch_to_S3_task