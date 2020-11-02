from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators import (CreateTableOperator, FetchApiOperator, StageToRedshiftOperator,
                               DataQualityOperator, LoadTableOperator)
from airflow.operators.dummy_operator import DummyOperator

from helpers import SqlQueries

s3_bucket = 'dataengineer-udacity'

song_s3_key = "song_data"
log_s3_key = "log_data"
log_json_file = "log_json_path.json"
aws_credentials = "aws_con"
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

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)
finish_operator = DummyOperator(task_id='finish_execution', dag=dag)

fetch_api_bisq = FetchApiOperator(task_id="fetch_api_bisq", dag=dag, aws_con=aws_credentials,
                                  remote_provider="bisq", aws_bucket_name=s3_bucket)
fetch_api_paxful = FetchApiOperator(task_id="fetch_api_paxful", dag=dag, aws_con=aws_credentials,
                                    remote_provider="paxful", aws_bucket_name=s3_bucket)

create_table = CreateTableOperator(task_id="Create_table", dag=dag, conn_id="redshift",
                                   sql_query=SqlQueries.create_table)

stage_paxful_to_redshift = StageToRedshiftOperator(
    task_id='stage_paxful',
    dag=dag,
    table_name="staging_paxful",
    s3_bucket=s3_bucket,
    conn_id="redshift",
    remote_provider="paxful",
    aws_credential_id=aws_credentials,
    provide_context=True
)

stage_bisq_to_redshift = StageToRedshiftOperator(
    task_id='stage_bisq',
    dag=dag,
    table_name="staging_bisq",
    s3_bucket=s3_bucket,
    conn_id="redshift",
    remote_provider="bisq",
    aws_credential_id=aws_credentials,
    provide_context=True
)

load_currency_table = LoadTableOperator(
    task_id='load_currency_table',
    dag=dag,
    conn_id="redshift",
    sql_query=[SqlQueries.staging_currency_table_insert, SqlQueries.currency_table_insert],
    empty_table=True,
    table_name="staging_currency",
)

load_time_table = LoadTableOperator(
    task_id='load_time_table',
    dag=dag,
    conn_id="redshift",
    sql_query=SqlQueries.time_table_insert,
    table_name="time",
)

load_transaction_table = LoadTableOperator(
    task_id='load_transaction_table',
    dag=dag,
    conn_id="redshift",
    sql_query=SqlQueries.transaction_table_insert,
    table_name="transaction",
)

run_quality_checks = DataQualityOperator(
    task_id='run_quality_checks',
    dag=dag,
    conn_id="redshift",
)

start_operator >> create_table >> [fetch_api_bisq, fetch_api_paxful]

fetch_api_bisq >> [stage_paxful_to_redshift, stage_bisq_to_redshift] >> [load_currency_table, load_time_table]
[load_currency_table, load_time_table] >> load_transaction_table
load_transaction_table >> run_quality_checks >> finish_operator
