from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udacity_data_pipelines',
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          default_args=default_args,
          end_date=datetime(2018, 11, 2)
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data/{{ dag_run.logical_date.year }}/{{ dag_run.logical_date.month }}/',
    json_path='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data/A/A/'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    sql_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    sql_query=SqlQueries.user_table_insert,
    truncate=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    sql_query=SqlQueries.song_table_insert,
    truncate=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    sql_query=SqlQueries.artist_table_insert,
    truncate=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    sql_query=SqlQueries.time_table_insert,
    truncate=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    quality_checks=[
        {'quality_sql': 'SELECT COUNT(*) FROM songplays WHERE playid IS null',
         'expected_result': 0},
        {'quality_sql': 'SELECT COUNT(*) FROM users WHERE userid IS null',
         'expected_result': 0},
        {'quality_sql': 'SELECT COUNT(*) FROM songs WHERE songid IS null',
         'expected_result': 0},
        {'quality_sql': 'SELECT COUNT(*) FROM artists WHERE artistid IS null',
         'expected_result': 0},
        {'quality_sql': 'SELECT COUNT(*) FROM time WHERE start_time IS null',
         'expected_result': 0}]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

staging_tasks = [stage_events_to_redshift,
                 stage_songs_to_redshift]

dimension_table_tasks = [load_user_dimension_table,
                         load_song_dimension_table,
                         load_artist_dimension_table,
                         load_time_dimension_table]

start_operator >> staging_tasks >> load_songplays_table
load_songplays_table >> dimension_table_tasks >> run_quality_checks
run_quality_checks >> end_operator
