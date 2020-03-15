from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from helpers.create_tables import CreateTablesQueries

default_args = {
    'owner': 'naqeeb',
    'depends_on_past': False,
    'catchup': False,
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id ='aws_credentials',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    region='us-west-2',
    file_type='JSON',
    json_format_file='s3://udacity-dend/log_json_path.json',
    create_table_stmt=CreateTablesQueries.staging_events_table_create
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id ='aws_credentials',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    region='us-west-2',
    file_type='JSON',
    create_table_stmt=CreateTablesQueries.staging_songs_table_create
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    sql_stmt=SqlQueries.songplay_table_insert,
    create_table_stmt=CreateTablesQueries.songplay_table_create
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    sql_stmt=SqlQueries.user_table_insert,
    truncate=True,
    create_table_stmt=CreateTablesQueries.user_table_create
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    sql_stmt=SqlQueries.song_table_insert,
    truncate=True,
    create_table_stmt=CreateTablesQueries.song_table_create
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    sql_stmt=SqlQueries.artist_table_insert,
    truncate=True,
    create_table_stmt=CreateTablesQueries.artist_table_create
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    sql_stmt=SqlQueries.time_table_insert,
    truncate=True,
    create_table_stmt=CreateTablesQueries.time_table_create
)

run_quality_checks_songplays = DataQualityOperator(
    task_id='Run_data_quality_checks_songplays',
    dag=dag,
    redshift_conn_id='redshift',
    table = 'songplays',
    test_stmts=["SELECT COUNT(*) FROM songplays WHERE user_id IS NULL"],
    test_results=[[(0,)]]

)

run_quality_checks_users = DataQualityOperator(
    task_id='Run_data_quality_checks_users',
    dag=dag,
    redshift_conn_id='redshift',
    table = 'users',
    test_stmts=["SELECT COUNT(*) FROM users WHERE first_name IS NULL"],
    test_results=[[(0,)]]

)

run_quality_checks_songs = DataQualityOperator(
    task_id='Run_data_quality_checks_songs',
    dag=dag,
    redshift_conn_id='redshift',
    table = 'songs',
    test_stmts=["SELECT COUNT(*) FROM songs WHERE title IS NULL","SELECT COUNT(*) FROM songs WHERE year < 1900 AND year > 2020"],
    test_results=[[(0,)],[(0,)]]

)

run_quality_checks_artists = DataQualityOperator(
    task_id='Run_data_quality_checks_artists',
    dag=dag,
    redshift_conn_id='redshift',
    table = 'artists',
    test_stmts=["SELECT COUNT(*) FROM artists WHERE name IS NULL"],
    test_results=[[(0,)]]

)

run_quality_checks_time = DataQualityOperator(
    task_id='Run_data_quality_checks_time',
    dag=dag,
    redshift_conn_id='redshift',
    table = 'time',
    test_stmts=["SELECT COUNT(*) FROM time WHERE year < 1900 AND year > 2020"],
    test_results=[[(0,)]]

)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
load_songplays_table >> run_quality_checks_songplays
load_user_dimension_table >> run_quality_checks_users
load_song_dimension_table >> run_quality_checks_songs
load_artist_dimension_table >> run_quality_checks_artists
load_time_dimension_table >> run_quality_checks_time

[run_quality_checks_songplays, run_quality_checks_users,run_quality_checks_songs, 
run_quality_checks_artists, run_quality_checks_time] >> end_operator 