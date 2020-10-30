from datetime import datetime, timedelta

import requests, json
from airflow.hooks import S3_hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class FetchApiOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 remote_provider: str = "",
                 aws_con: str = "",
                 aws_key: str = "",
                 aws_bucket_name: str = "",
                 *args, **kwargs):
        super(FetchApiOperator, self).__init__(*args, **kwargs)
        self.remote_provider = remote_provider
        self.aws_con = aws_con
        self.aws_key = aws_key
        self.aws_bucket_name = aws_bucket_name

    def execute(self, context):
        self.start = int(datetime.strptime(context["ds"], "%Y-%m-%d").timestamp())
        self.end = int((datetime.strptime(context["ds"], "%Y-%m-%d") + timedelta(days=1)).timestamp())
        self.data = []
        filename = f"{self.remote_provider}({context['ds']}).json"

        self.fetch_url()

        open('/tmp/' + filename, 'wb').write(json.dumps(self.data))

        hook = S3_hook.S3Hook(self.aws_con)
        hook.load_file('/tmp/' + filename, filename, self.aws_bucket_name)

    def fetch_url(self, count=0):
        url = None
        pagination = 0
        if self.remote_provider == "bisq":
            url = f"https://bisq.markets/api/trades?market=all&" \
                  f"timestamp_from={self.start}&timestamp_to={self.end}&limit=2000"
            pagination = 2000
        elif self.remote_provider == "paxful":
            url = f"https://paxful.com/data/trades?sincetype=date&since={self.start}"
            pagination = 100

        if url is not None:
            response = requests.request("GET", url)
            self.log.info(f"URL: {url}")
            self.log.info(f"Status code: {response.status_code}")
            data = response.json()
            if self.remote_provider == "bisq":
                self.data += data
            elif self.remote_provider == "paxful":
                self.data += list(x for x in data if int(x["date"]) < self.end)
            if len(data) > 0:
                self.start = self.data[0]["date"]
                if len(data) >= pagination:
                    self.fetch_url( count+1)


