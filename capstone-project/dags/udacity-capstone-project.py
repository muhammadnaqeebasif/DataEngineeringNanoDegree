from datetime import datetime, timedelta
import logging
from operators import *
from helpers import *
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from airflow import settings
from airflow.models import Connection
from aws_configuration_parser import *
from airflow.operators.python_operator import PythonOperator

def hello_world():
    logging.info(f"Hello World! {S3['bucket']}")


# conn = Connection(conn_id='redshift',
#                   conn_type='postgres',
#                   host=)

default_args = {
    'owner': 'naqeeb',
    'depends_on_past': False,
    'catchup': False,
    'start_date': datetime(2020, 3, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
        'udacity-capstone-project',
        default_args=default_args,
        description='Load and transform data in Redshift with Airflow',
        schedule_interval='@hourly'
        )

# start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# stage_events_to_redshift = StageToRedshiftOperator(
#     task_id='Stage_events',
#     dag=dag,
#     redshift_conn_id='redshift',
#     aws_credentials_id ='aws_credentials',
#     table='staging_events',
#     s3_bucket='udacity-dend',
#     s3_key='log_data',
#     region='us-west-2',
#     file_type='JSON',
#     create_table_stmt=CreateTablesQueries.staging_neighborhoods_table_create
# )

greet_task = PythonOperator(
   task_id="hello_world_task",
   python_callable=hello_world,
   dag=dag
)