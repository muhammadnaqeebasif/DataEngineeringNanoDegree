from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """ An airflow custom operator which loads the fact table from the staged tables.

    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 sql_stmt,
                 create_table_stmt=None,
                 *args, **kwargs):
        """LoadFactOperator Constructor to inialize the object.
        
        Parameters
        ----------
        redshift_conn_id : str
            redshift connection id used by the Postgresql hook
        table : str
            The fact table
        sql_stmt : str
            SQL statement which specifies how to load fact table from the staged tables
        create_table_stmt : str, optional
            If speficied the table is first created according to the
            statement provided, by default None
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt = sql_stmt
        self.create_table_stmt = create_table_stmt

    def execute(self, context):
        """ The function which is called by default by Airflow.
        
        Parameters
        ----------
        context : dict
            Contains the context properties
        """
        # creates a postgresql hook
        redshift = PostgresHook(self.redshift_conn_id)

        # If create_table_stmt is specified then the table is created first
        if self.create_table_stmt:
            self.log.info(f'Creating {self.table} in Redshift')
            redshift.run(self.create_table_stmt)

        # Loads the data into the fact table according to the statement specified
        formatted_sql = f"INSERT INTO {self.table} ({self.sql_stmt})"
        redshift.run(formatted_sql)

        # If loading to the table is successful then print Success to the logs
        self.log.info(f"Success: Loading {self.table}")