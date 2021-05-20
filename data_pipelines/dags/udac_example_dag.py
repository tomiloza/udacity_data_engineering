from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from udacity_plugin.operators.data_quality import DataQualityOperator
from udacity_plugin.operators.load_fact import LoadFactOperator
from udacity_plugin.operators.load_dimension import LoadDimensionOperator
from udacity_plugin.operators.stage_redshift import StageToRedshiftOperator
from udacity_plugin.helpers.sql_queries import SqlQueries
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'tomislav-lozancic',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_tables_task = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    sql=SqlQueries.create_table_queries,
    postgres_conn_id="redshift"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    region='us-west-2',
    json_path='s3://udacity-dend/log_json_path.json',
    execution_date='{{ execution_date }}'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    region='us-west-2',
    execution_date='{{ execution_date }}'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    sql=SqlQueries.user_table_insert,
    delete_all_rows=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    sql=SqlQueries.song_table_insert,
    delete_all_rows=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    sql=SqlQueries.artist_table_insert,
    delete_all_rows=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    sql=SqlQueries.time_table_insert,
    delete_all_rows=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    sql_check=[
        "SELECT COUNT(*) FROM songplays WHERE userid IS NULL",
        "SELECT COUNT(*) FROM artists WHERE name IS NULL",
        "SELECT COUNT(*) FROM songs WHERE title IS NULL",
        "SELECT COUNT(*) FROM users WHERE first_name IS NULL",
        "SELECT COUNT(*) FROM time WHERE weekday IS NULL",
    ],
    expected_results=[0, 0, 0, 0, 0]

)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> stage_songs_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
run_quality_checks >> end_operator
