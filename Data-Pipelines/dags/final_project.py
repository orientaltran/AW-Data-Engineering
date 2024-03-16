from datetime import datetime, timedelta
import pendulum
import os
from airflow import DAG

# Import function create database
from functions.create_table import CreateTableOperator

# Import from functions common
from functions.sql_queries import SqlQueries
from functions.load_fact import LoadFactOperator
from functions.load_dimension import LoadDimensionOperator
from functions.data_quality import DataQualityOperator
from functions.drop_table import DropTableOperator
from functions.stage_redshift import StageToRedshiftOperator


default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': pendulum.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Init
dag = DAG('final-dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

# Begin_execution
start_operator = CreateTableOperator(
    task_id='Begin_execution',
    dag=dag,
    connection_id="redshift",
    sql_file="/home/workspace/airflow/dags/functions/create_tables.sql"
)

# Stage_events
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    region="us-east-1",
    s3={
        "bucket_name": "datapepline",
        "prefix": "log-data"
    },
    connection_id={
        "credentials": "aws_credentials",
        "redshift": "redshift"
    }
)

# Staging_songs
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='staging_songs',
    dag=dag,
    table="staging_songs",
    region="us-east-1",
    s3={
        "bucket_name": "datapepline",
        "prefix": "song-data"
    },
    connection_id={
        "credentials": "aws_credentials",
        "redshift": "redshift"
    }
)

# Load_songplays_fact_table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    connection_id="redshift",
    table="songplays",
    sql_query=SqlQueries.songplay_table_insert
)

# Load_user_dim_table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    connection_id="redshift",
    table="users",
    sql_query=SqlQueries.user_table_insert
)

# Load_song_dim_table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    connection_id="redshift",
    table="songs",
    sql_query=SqlQueries.song_table_insert
)

# Load_artist_dim_table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    connection_id="redshift",
    table="artists",
    sql_query=SqlQueries.artist_table_insert
)

# Load_time_dim_table
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    connection_id="redshift",
    table="time",
    sql_query=SqlQueries.time_table_insert
)

# Run_data_quality_checks
run_data_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["songplays", "artists", "time", "songs", "users"]
)

# End_execution
end_execution = DropTableOperator(task_id='Stop_execution', dag=dag, connection_id="redshift",
                                        tables=["artists", "songplays", "songs", "time", "users"])

start_operator >> 
[ 
    stage_events_to_redshift, 
    stage_songs_to_redshift 
] >> 
    load_songplays_table 
>> [
    load_user_dimension_table,
    load_song_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table
] >> run_data_quality_checks >> end_execution
