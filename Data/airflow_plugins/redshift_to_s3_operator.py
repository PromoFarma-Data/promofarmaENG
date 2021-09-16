from typing import List, Optional, Union

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class RedshiftToS3Operator(BaseOperator):

    template_fields = ('s3_key', 's3_bucket', 'table', 'redshift_conn_id', 'query',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    def __init__(
            self,
            s3_key: str,
            schema: Optional[str] = None,
            table: Optional[str] = None,
            query: Optional[str] = None,
            s3_bucket: str = "{{ var.json.get('variables_secret').s3_bucket }}",
            redshift_conn_id: str = "{{ var.json.get('variables_secret').redshift_conn_id }}",
            aws_conn_id: str = 'aws_default',
            verify: Optional[Union[bool, str]] = None,
            unload_options: Optional[List] = None,
            autocommit: bool = True,
            include_header: bool = False,
            include_load_datetime: bool = False,
            load_datetime_value: str = 'NULL',
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.unload_options = unload_options or []  # type: List
        self.autocommit = autocommit
        self.include_header = include_header
        self.schema = schema
        self.table = table
        self.query = query
        self.include_load_datetime = include_load_datetime
        self.load_datetime_value = load_datetime_value

        if self.include_header and 'HEADER' not in [uo.upper().strip() for uo in self.unload_options]:
            self.unload_options = list(self.unload_options) + ['HEADER', ]

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        unload_options = '\n\t\t\t'.join(self.unload_options)

        if not self.query:
            if self.include_load_datetime:
                self.query = f"SELECT *, {self.load_datetime_value} AS load_datetime FROM {self.schema}.{self.table}"
            if not self.include_load_datetime:
                self.query = f"SELECT * FROM {self.schema}.{self.table}"

        unload_query = """
                    UNLOAD ($${select_query}$$)
                    TO 's3://{s3_bucket}/{s3_key}'
                    iam_role 'arn:aws:iam::role'
                    {unload_options};
                    """.format(select_query=self.query,
                               s3_bucket=self.s3_bucket,
                               s3_key=self.s3_key,
                               unload_options=unload_options)

        self.log.info('Executing UNLOAD command...')
        postgres_hook.run(unload_query, self.autocommit)
        self.log.info("UNLOAD command complete...")
