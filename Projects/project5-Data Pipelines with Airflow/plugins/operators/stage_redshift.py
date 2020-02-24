from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)

    copy_sql = """
        COPY {table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_key}'
        {file_format_smt}
        TIMEFORMAT AS 'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
        REGION '{region}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id ='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 region='us-west-2',
                 file_type='JSON',
                 json_format_file='auto',
                 delimiter=',',
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id =aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.file_type = file_type
        self.json_format_file = json_format_file
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Copying data from S3 to Redshift')
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        if self.file_type.lower() =='csv':
            file_format_smt = "CSV IGNOREHEADER {} DELIMITER '{}'".format(self.ignore_headers,
                                                                        self.delimiter)
        else:
            file_format_smt = "JSON '{}'".format(self.json_format_file)
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            table=self.table,
            s3_path=s3_path,
            access_key=credentials.access_key,
            secret_key=credentials.secret_key,
            file_format_smt=file_format_smt,
            region=self.region
        )
        
        redshift.run(formatted_sql)
        self.log.info(f"Success: Copying {self.table} from S3 to Redshift")