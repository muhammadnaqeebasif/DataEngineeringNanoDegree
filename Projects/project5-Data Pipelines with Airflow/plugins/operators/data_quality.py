from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 test_stmt=None,
                 result=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.test_stmt = test_stmt
        self.result = result

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        records = redshift.run(f"SELECT COUNT(*) FROM {self.table}")

        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError((f"Data quality check failed. {self.table} returned no results"))
        
        num_records = records[0][0]

        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        self.log.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")

        if self.test_stmt:
            results = redshift.run(self.test_stmt)
            if self.result != results:
                raise ValueError(f"Data quality check failed: {results} != {self.result}") 

            self.log.info(f"Data quality on table {self.table} check passed with {self.test_stmt} results")
        