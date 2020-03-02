from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """ An airflow custom operator to check data quality in a table according to the
    tests provided. By default the only data quality check which checks whether the
    number of records in the table is 0 or not.
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 test_stmts=None,
                 test_results=None,
                 *args, **kwargs):
        """DataQualityOperator Constructor to inialize the object.
        
        Parameters
        ----------
        redshift_conn_id : str
            redshift connection id used by the Postgresql hook
        table : str
            The table which needs to pass through quality checking
        test_stmts : list, optional
            List of test statements to run, by default None
        test_results : list, optional
            List of results of the test statement, by default None
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.test_stmts = test_stmts
        self.test_results = test_results

    def execute(self, context):
        """The function which is called by default by Airflow.
        
        Parameters
        ----------
        context : dict
            Contains the context properties
        
        Raises
        ------
        ValueError
            If number of records in the table is zero
        ValueError
            If the output returned by any test statement is not equal to the specified
            result
        """
        # creates a postgresql hook
        redshift = PostgresHook(self.redshift_conn_id)
        # Runs check if the table contains 0 record or not
        records = redshift.get_records(f"SELECT COUNT(*) FROM {self.table}")

        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError((f"Data quality check failed. {self.table} returned no results"))
        
        num_records = records[0][0]

        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        
        self.log.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")

        # if test statements are specified then the tests are run
        if self.test_stmts:
            for i,test_stmt in enumerate(self.test_stmts):
                self.log.info(f"Performing data quality check with query: {test_stmt}")
                
                results = redshift.get_records(test_stmt)
                
                if self.test_results[i] != results:
                    raise ValueError(f"Data quality check failed: {results} not equal to {self.test_results[i]}") 

                self.log.info(f"Data quality on table {self.table} passed with results")