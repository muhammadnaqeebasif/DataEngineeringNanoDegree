from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """ An airflow operator which stages s3 data to the redshift table.

    Attributes
    ----------
    copy_sql : str
        A sql template used for staging the data from S3 path to the table.
        It requires table, path to s3, credentials, file_formats and regions.

    """

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
                 redshift_conn_id,
                 aws_credentials_id,
                 table,
                 s3_bucket,
                 s3_key,
                 region='us-west-2',
                 file_type='JSON',
                 json_format_file='auto',
                 delimiter=',',
                 ignore_headers=1,
                 create_table_stmt=None,
                 drop_table = False,
                 *args, **kwargs):
        """StageToRedshiftOperator Constructor to inialize the object.
        
        Parameters
        ----------
        redshift_conn_id : str
            redshift connection id used by the Postgresql hook.
        aws_credentials_id : str
            AWS credential id used by Aws hooks.
        table : str
            The table to which we want to stage the data to.
        s3_bucket : str
            S3 bucket from where the data resides.
        s3_key : str
            S3 key where the data resides.
        region : str, optional
            The aws region, by default 'us-west-2'
        file_type : str, optional
            The types of the file in which data is stored, by default 'JSON'
        json_format_file : str, optional
            If type of the file is JSON then json_format_file will give the 
            aoth to format file, by default 'auto'
        delimiter : str, optional
            Delimiter in CSV data, by default ','
        ignore_headers : int, optional
            To specify if headers should be igonored in the CSV data, by default 1
        create_table_stmt : str, optional
            If speficied the table is first created according to the
            statement provided, by default None
        drop_table : bool, optional
            If true then the table is dropped first
        """

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
        self.create_table_stmt = create_table_stmt
        self.drop_table = drop_table

    def execute(self, context):
        """ The function which is called by default by Airflow.
        
        Parameters
        ----------
        context : dict
            Contains the context properties
        
        Raises
        ------
        ValueError
            If the file type specified is not CSV or JSON
        """
        # Creates the aws_hook
        aws_hook = AwsHook(self.aws_credentials_id)
        # Gets credentials from aws_hook
        credentials = aws_hook.get_credentials()
        # creates a postgresql hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # If drop_table is True then table is dropped first
        if self.drop_table:
            self.log.info(f'Dropping {self.table} in Redshift')
            redshift.run(f"DROP TABLE IF EXISTS {self.table}")

        # If create_table_stmt is specified then the table is created first
        if self.create_table_stmt:
            self.log.info(f'Creating {self.table} in Redshift')
            redshift.run(self.create_table_stmt)

        self.log.info('Copying data from S3 to Redshift')
        ## Makes the path by rendering key and combining keu and bucket accordingly
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        # Creates file_format_smt according to the type of file specified
        if self.file_type.lower() =='csv':
            file_format_smt = "CSV IGNOREHEADER {} DELIMITER '{}'".format(self.ignore_headers,
                                                                        self.delimiter)
        elif self.file_type.lower() =='json':
            file_format_smt = "JSON '{}'".format(self.json_format_file)
        else:
            raise ValueError(f"Invalid file type: {self.file_type}. Only acceptable file types are json and csv.")

        # Creates the SQL statement according to the values specified
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            table=self.table,
            s3_path=s3_path,
            access_key=credentials.access_key,
            secret_key=credentials.secret_key,
            file_format_smt=file_format_smt,
            region=self.region
        )
        
        # Stages the data
        redshift.run(formatted_sql)

        # If copying is successful then print Success to the logs
        self.log.info(f"Success: Copying {self.table} from S3 to Redshift")