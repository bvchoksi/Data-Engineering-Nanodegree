from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.models import Connection

aws_credentials = Connection(
    conn_id = "aws_credentials",
    conn_type = "Amazon Web Services",
    host = "",
    login = "",
    password = ""
)

redshift = Connection(
    conn_id = "redshift",
    conn_type = "Postgres",
    host = "redshift-cluster-1.cnkhcohvzift.us-west-2.redshift.amazonaws.com",
    login = "",
    password = "",
    port = "5439",
    schema = "dev"
)

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag) 

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    dag = dag,
    table = "staging_events",
    redshift_conn_id = redshift,
    aws_credentials_id = aws_credentials,
    s3_bucket = "udacity-dend",
    s3_key = "log_data",
    json_path = "auto"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    dag = dag,
    table = "staging_songs",
    redshift_conn_id = redshift,
    aws_credentials_id = aws_credentials,
    s3_bucket = "udacity-dend",
    s3_key = "song_data",
    json_path = "auto"
)

load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    table = "songplays",
    truncate = "n",
    redshift_conn_id = redshift,
    sql = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id = "Load_user_dim_table",
    dag = dag,
    table = "users",
    truncate = "y",
    redshift_conn_id = redshift,
    sql = SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = "Load_song_dim_table",
    dag = dag,
    table = "songs",
    truncate = "y",
    redshift_conn_id = redshift,
    sql = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = "Load_artist_dim_table",
    dag = dag,
    table = "artists",
    truncate = "y",
    redshift_conn_id = redshift,
    sql = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = "Load_time_dim_table",
    dag = dag,
    table = "time",
    truncate = "y",
    redshift_conn_id = redshift,
    sql = SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id = 'Run_data_quality_checks',
    dag = dag,
    dq_checks = [
        {
            "check_name": "Has Rows",
            "check_sql": "select case when count(*) > 0 then 1 else 0 end as has_rows from {}",
            "expected_result": 1
        }
    ],
    tables = ["songplays", "users", "songs", "artists", "time"],
    redshift_conn_id = redshift
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator