import requests
from airflow.hooks import S3_hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class FetchApiOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 aws_con: str = "",
                 aws_key: str = "",
                 aws_bucket_name: str = "",
                 *args, **kwargs):
        super(FetchApiOperator, self).__init__(*args, **kwargs)
        self.aws_con = aws_con
        self.aws_key = aws_key
        self.aws_bucket_name = aws_bucket_name

    def execute(self, context):
        url = "https://bisq.markets/api/trades?market=all&timestamp_from=1603656800&timestamp_to=1603756800&limit=2000"

        response = requests.request("GET", url)
        filename = '/tmp/bisq.json'
        open(filename, 'wb').write(response.content)

        hook = S3_hook.S3Hook(self.aws_con)
        hook.load_file(filename, self.aws_key, self.aws_bucket_name)
