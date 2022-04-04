from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key',)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 file_format='json',
                 json_path='auto',
                 delimiter=',',
                 ignore_headers=True,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.json_path = json_path
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers

    def execute(self, context):
        aws_hook = AwsBaseHook(aws_conn_id=self.aws_credentials_id, client_type='redshift')
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'Clearing data from "{self.table}" Redshift table')
        redshift.run(f'DELETE FROM {self.table}')

        self.log.info('Copying data from S3 to Redshift')
        if self.file_format == 'json':
            file_type_sql_command = f"JSON '{self.json_path}'"
        elif self.file_format == 'csv':
            file_type_sql_command = f"IGNOREHEADER '{self.ignore_headers}'\
                                      DELIMITER '{self.delimiter}'"
        else:
            self.log.error(f'File format "{self.file_format}" not recognized')

        rendered_key = self.s3_key.format(**context)
        s3_path = f's3://{self.s3_bucket}/{rendered_key}'
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            file_type_sql_command
        )
        redshift.run(formatted_sql)
