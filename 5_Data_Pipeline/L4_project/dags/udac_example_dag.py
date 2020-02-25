from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Variable

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')
s3_bucket = Variable.get('s3_bucket')
json_path = Variable.get('json_path')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    #'end_date': datetime(2018, 11, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
}

dag = DAG('sparkify_airflow_dag',
          default_args=default_args,
          description='Load and transform sparkify data in Redshift with Airflow',
          schedule_interval='@hourly' # Run once an hour at the beginning of the hour
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'staging_events',
    s3_bucket = s3_bucket,
    s3_key = 'log_data/{execution_date.year}/{execution_date.month}/',
    json = json_path,
    provide_context=True,
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'staging_songs',
    s3_bucket = s3_bucket,
    s3_key = 'song_data/',
    provide_context=True,
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'songplays',
    sql_querie = SqlQueries.songplay_table_insert,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'users',
    sql_querie = SqlQueries.user_table_insert,
    insert_mode = 'append',
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'songs',
    sql_querie = SqlQueries.song_table_insert,
    insert_mode = 'append',
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'artists',
    sql_querie = SqlQueries.artist_table_insert,
    insert_mode = 'append',
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'time',
    sql_querie = SqlQueries.time_table_insert,
    insert_mode = 'append',
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    dq_checks=[
        {'check_sql': "SELECT (COUNT(*)/COUNT(*)) AS quotient FROM users", 'expected_result': 1},
        {'check_sql': "SELECT (COUNT(*)/COUNT(*)) AS quotient FROM songs", 'expected_result': 1},
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# configuration of the DAG
# first create tables
start_operator >> create_tables
# parallel staging of tables
create_tables >> stage_events_to_redshift
create_tables >> stage_songs_to_redshift
# serial loading of fact table
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
# parallel loading of dimension tables
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
# serial checking of data quality
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
# closing DAG
run_quality_checks >> end_operator