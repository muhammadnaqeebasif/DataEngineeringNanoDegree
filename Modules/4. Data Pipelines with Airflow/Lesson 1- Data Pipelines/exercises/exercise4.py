
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks import S3_hook
from airflow.models import Variable

import datetime
import logging

def list_keys():
    hook = S3_hook.S3Hook('aws_credentials')
    bucket = Variable.get('s3_bucket')
    prefix = Variable.get('s3_prefix')
    logging.info(f"Listing Keys from {bucket}/{prefix}")
    keys = hook.list_keys(bucket, prefix=prefix)
    for key in keys:
        logging.info(f"- s3://{bucket}/{key}")

dag = DAG('lesson1.exercise4', start_date=datetime.datetime.now() - datetime.timedelta(days=1))

list_task = PythonOperator(
    task_id="list_keys",
    python_callable=list_keys,
    dag=dag
)

list_task
