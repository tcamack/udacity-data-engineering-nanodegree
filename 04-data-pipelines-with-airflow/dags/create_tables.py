import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from udacity_data_pipelines import default_args

dag = DAG('create_redshift_tables',
          default_args=default_args,
          start_date=datetime.datetime.now(),
          schedule_interval=None)

create_tables = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id='redshift',
    sql='create_tables.sql'
)
