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
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
        'udacity-capstone-project',
        default_args=default_args,
        description='Load and transform data in Redshift with Airflow',
        schedule_interval='@once'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
ending_operator = DummyOperator(task_id='End_execution',dag=dag)
# creating a list of tables
tables = ['forces','neighborhoods','crimes','date','senior_officers',
          'neighborhood_locations','neighborhood_boundaries','outcomes']

# path S3 which each staging table is loaded
staging_tables_dict = {}

for table in tables:
    if table == 'date':
        continue
    elif table in ['crimes','outcomes']:
        prefix = aws_configs.S3['real_processed_key']
    else:
        prefix = aws_configs.S3['batched_process_key']
    staging_tables_dict[table] = (f"{prefix}/{table}",getattr(CreateTableQueries,f"staging_{table}_table_create"))

staging_to_redshift_operations = {}

for table,(s3_key,create_table_stmt) in staging_tables_dict.items():
    staging_to_redshift_operations[table] = StageToRedshiftOperator(
                                                    task_id=f'Stage_{table}',
                                                    dag=dag,
                                                    redshift_conn_id='redshift',
                                                    aws_credentials_id ='aws_credentials',
                                                    table=f'staging_{table}',
                                                    s3_bucket=aws_configs.S3['BUCKET'],
                                                    s3_key=s3_key,
                                                    region=aws_configs.REGION,
                                                    file_type='JSON',
                                                    create_table_stmt=create_table_stmt,
                                                    drop_table = True
                                                )

table_insert_operations = {}
for table in tables[:4]:
    if table == 'forces':
        sql_stmt = SQLQueries.stage_table_insert.format(source=f"staging_{table}",
                                                        cols="*",
                                                        target=f"dim_{table}")
    else:
        sql_stmt = getattr(SQLQueries,f"{table}_table_insert")
    table_insert_operations[table] = LoadDimensionOperator(
                                        task_id=f'Load_{table}_dim_table',
                                        dag=dag,
                                        redshift_conn_id='redshift',
                                        table=f"dim_{table}",
                                        sql_stmt=sql_stmt,
                                        create_table_stmt=getattr(CreateTableQueries,f"dim_{table}_table_create")
                                    ) 

for table in tables[4:7]:
    insert_stmt = getattr(SQLQueries,f"{table}_insert_stmt")
    sql_stmt = SQLQueries.stage_table_insert.format(source=f"staging_{table}",
                                                    cols= ','.join(getattr(SQLQueries,f"{table}_cols")),
                                                    target=table)
    table_insert_operations[table] = LoadDimensionOperator(
                                        task_id=f'Load_{table}_table',
                                        dag=dag,
                                        redshift_conn_id='redshift',
                                        table=table,
                                        insert_stmt=insert_stmt,
                                        sql_stmt=sql_stmt,
                                        create_table_stmt=getattr(CreateTableQueries,f"{table}_table_create")
                                    )

table_insert_operations['outcomes'] = LoadFactOperator(
                            task_id=f'Load_fact_outcomes_table',
                            dag=dag,
                            redshift_conn_id='redshift',
                            table="fact_outcomes",
                            sql_stmt=SQLQueries.outcomes_table_insert,
                            insert_stmt=SQLQueries.outcomes_insert_stmt,
                            create_table_stmt= CreateTableQueries.fact_outcomes_table_create
                        )

data_quality_operations = {}
for table in ['forces','neighborhoods','crimes','date']:
    data_quality_operations[table] = DataQualityOperator(
                                        task_id=f'Run_data_quality_checks_dim_{table}',
                                        dag=dag,
                                        redshift_conn_id='redshift',
                                        table = f"dim_{table}"
                                     )

for table in ['senior_officers','neighborhood_locations','neighborhood_boundaries']:
    data_quality_operations[table] = DataQualityOperator(
                                        task_id=f'Run_data_quality_checks_{table}',
                                        dag=dag,
                                        redshift_conn_id='redshift',
                                        table = table
                                     )

data_quality_operations['outcomes'] = DataQualityOperator(
                                        task_id=f'Run_data_quality_checks_fact_outcomes',
                                        dag=dag,
                                        redshift_conn_id='redshift',
                                        table = 'fact_outcomes'
                                     ) 

start_operator >> [operation for key,operation in staging_to_redshift_operations.items()]

staging_to_redshift_operations['forces'] >> table_insert_operations['forces']
staging_to_redshift_operations['neighborhoods'] >> table_insert_operations['neighborhoods']
staging_to_redshift_operations['crimes'] >> table_insert_operations['crimes']
[staging_to_redshift_operations['crimes'],staging_to_redshift_operations['outcomes']] >> table_insert_operations['date']

for table in ['forces','neighborhoods','crimes','date']:
    table_insert_operations[table] >> table_insert_operations['outcomes']
    for other_tables in tables[4:7]:
        table_insert_operations[table] >> table_insert_operations[other_tables]

for table in tables:
    table_insert_operations[table] >> data_quality_operations[table]

for table in tables:
    data_quality_operations[table] >> ending_operator
