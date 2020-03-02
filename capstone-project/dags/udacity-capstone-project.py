from datetime import datetime, timedelta
import logging
import os
from operators import *
from helpers import *
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from airflow import settings
from airflow.models import Connection
from aws_configuration_parser import *
from airflow.operators.python_operator import PythonOperator
 
# creating aws configuration object
aws_configs = AwsConfigs(f"{os.environ['AIRFLOW_HOME']}/credentials/credentials.csv", 
                         f"{os.environ['AIRFLOW_HOME']}/credentials/resources.cfg")

def hello_world():
    logging.info(f"Hello World! {aws_configs.S3['bucket']}")

# Creating redshift connection
redshift_conn = Connection(conn_id='redshift',
                  conn_type='postgres',
                  host=aws_configs.REDSHIFT['endpoint'],
                  login=aws_configs.REDSHIFT['db_user'],
                  password=aws_configs.REDSHIFT['db_password'],
                  schema=aws_configs.REDSHIFT['db_name'],
                  port=aws_configs.REDSHIFT['port']
                  )

# Creating aws connection
aws_conn = Connection(conn_id='aws_credentials',
                  conn_type='aws',
                  login=aws_configs.ACCESS_KEY,
                  password=aws_configs.SECRET_KEY
                  )

session = settings.Session() # get the session
session.add(redshift_conn)
session.add(aws_conn)

session.commit() # it will insert the connection object programmatically.



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

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

staging_neighborhoods_to_redshift = StageToRedshiftOperator(
    task_id='Stage_neighborhood',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id ='aws_credentials',
    table='staging_neighborhoods',
    s3_bucket=aws_configs.S3['BUCKET'],
    s3_key=f"{aws_configs.S3['batched_process_key']}/dim_neighborhoods",
    region=aws_configs.REGION,
    file_type='JSON',
    create_table_stmt=CreateTableQueries.staging_neighborhoods_table_create
)

start_operator >> staging_neighborhoods_to_redshift