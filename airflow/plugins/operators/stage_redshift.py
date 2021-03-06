from airflow.hooks import S3_hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    This operator load the information from the raw JSON data on S3 bucket to the redshift table.
    """

    ui_color = '#2ecc71'

    @apply_defaults
    def __init__(self,
                 conn_id: str = "",
                 aws_credential_id: str = "",
                 table_name: str = "",
                 s3_bucket: str = "",
                 remote_provider: str = "",
                 log_json_file: str = "",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.aws_credential_id = aws_credential_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.remote_provider = remote_provider
        self.log_json_file = log_json_file
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):

        base_copy_query = " COPY {} FROM '{}' ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}' region 'us-east-1' FORMAT AS json 'auto';"

        filename = f"{self.remote_provider}({context['ds']}).json"

        hook = S3_hook.S3Hook(self.aws_credential_id)
        credentials = hook.get_credentials()
        total = hook.read_key(filename, self.s3_bucket).strip().count('\n')

        if total > 0:
            s3_path = f"s3://{self.s3_bucket}/{filename}"

            copy_query = base_copy_query.format(self.table_name, s3_path, credentials.access_key,
                                                credentials.secret_key)

            self.log.info(f"Running copy: {copy_query}")
            redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)

            redshift_hook.run(copy_query)
            self.log.info(f"Table {self.table_name} ready!")
        else:
            self.log.info(f"Table {self.table_name} Empty!")
