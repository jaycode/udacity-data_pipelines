from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator,
                               PostgresOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup':False
}

# If True, do not delete and recreate previous table.
DIMENSION_APPEND_ONLY = False

dag = DAG('etl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1 # Very important parameter. Make sure we only run one thread at a time.
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id="Create_staging_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_staging_tables,
    # This task may fail, so we set retries to 1, and move on to next task when it does fail.
    retries=0
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_conn_id='aws_credentials',
    db_conn_id='redshift',
    table='staging_events',
    data_path='s3://udacity-dend/log_data',
    jsonpaths_path='s3://udacity-dend/log_json_path.json',
    # Create_tables may fail if tables already exist, and by then we want to run this task still.
    trigger_rule='all_done',
    count_query=SqlQueries.staging_events_count
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_conn_id='aws_credentials',
    db_conn_id='redshift',
    table='staging_songs',
    data_path='s3://udacity-dend/song_data',
    # Create_tables may fail if tables already exist, and by then we want to run this task still.
    trigger_rule='all_done',
    count_query=SqlQueries.staging_songs_count
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    conn_id='redshift',
    table='songplays',
    query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    conn_id='redshift',
    table='users',
    query=SqlQueries.user_table_insert,
    append_only=DIMENSION_APPEND_ONLY,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    conn_id='redshift',
    table='songs',
    query=SqlQueries.song_table_insert,
    append_only=DIMENSION_APPEND_ONLY,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    conn_id='redshift',
    table='artists',
    query=SqlQueries.artist_table_insert,
    append_only=DIMENSION_APPEND_ONLY,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    conn_id='redshift',
    table='time',
    query=SqlQueries.time_table_insert,
    append_only=DIMENSION_APPEND_ONLY,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id='redshift',
    non_empty_tables=['staging_songs', 'staging_events', 'songplays',
                      'songs', 'users', 'artists', 'time']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables >> \
[stage_events_to_redshift, stage_songs_to_redshift] >> \
load_songplays_table >> [load_song_dimension_table,
                         load_user_dimension_table,
                         load_artist_dimension_table,
                         load_time_dimension_table] >> \
run_quality_checks >> end_operator