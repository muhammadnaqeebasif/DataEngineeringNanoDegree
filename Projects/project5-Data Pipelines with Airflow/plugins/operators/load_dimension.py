from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql_stmt='',
                 truncate=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt = sql_stmt
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        if self.truncate:
            redshift.run(f"TRUNCATE TABLE {self.table}")
        
        formatted_sql = f"INSERT INTO {self.table} ({self.sql_stmt})"
        redshift.run(formatted_sql)
        self.log.info(f"Success: Loading {self.table}")