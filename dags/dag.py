from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries
import logging

S3_BUCKET = 'udacity-dend'
S3_SONG_KEY = 'song_data'
S3_LOG_KEY = 'log_data/{execution_date.year}/{execution_date.month}'
LOG_JSON_PATH = f's3://{S3_BUCKET}/log_json_path.json'
REGION = 'us-west-2'
AWS_CREDENTIALS_ID = 'aws_credentials'
REDSHIFT_CONN_ID = 'redshift'
DAG_ID = 'dag'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date':pendulum.datetime(2018, 11, 1, 0, 0, 0, 0),
    'end_date': pendulum.datetime(2018, 11, 2, 0, 0, 0, 0),
    'email_on_retry': False,
    'catchup': False,
}

class DataQualityTest:
    """Test case for data quality"""

    def __init__(self, sql, validation, table=None):
        """Initialize test case

        args:
            sql: SQL statement to execute
            validation: Validation method for query results
            table: Table name
        """
        self.sql = sql
        self.validation = validation
        self.table = table
        self.records = None

    def validate(self):
        return self.validation(self.records, self.table)

    @staticmethod
    def no_results_validation(records, table=None):
        """Validate non-empty table

        args:
            records: Query results
            table: Table name
        returns:
            True if test passed
        """
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f'Error: Table {table} returns no results')
        n_records = records[0][0]
        if n_records < 1:
            raise ValueError(f'Error: Table {table} contains 0 records')
        logging.info(f'Passed: Table {table} contains {n_records} records')
        return True

    @staticmethod
    def no_results_test(table):
        """Create test case to verify that table is not empty

        args:
            table: Table name

        returns:
            DataQualityTest
        """
        return DataQualityTest(
            sql=f'SELECT count(*) FROM {table}',
            validation=DataQualityTest.no_results_validation,
            table=table,
        )

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        conn_id=REDSHIFT_CONN_ID,
        aws_credentials_id=AWS_CREDENTIALS_ID,
        table='staging_events',
        s3_bucket=S3_BUCKET,
        s3_key=S3_LOG_KEY,
        region=REGION,
        truncate=False,
        data_format=f"JSON '{LOG_JSON_PATH}'",
        sql=SqlQueries.copy_sql
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        conn_id=REDSHIFT_CONN_ID,
        aws_credentials_id=AWS_CREDENTIALS_ID,
        table='staging_songs',
        s3_bucket=S3_BUCKET,
        s3_key=S3_SONG_KEY,
        region=REGION,
        truncate=True,
        data_format=f"JSON 'auto'",
        sql=SqlQueries.copy_sql
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        conn_id=REDSHIFT_CONN_ID,
        sql=SqlQueries.songplay_table_insert,
        table='songplays',
        truncate=False,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        conn_id=REDSHIFT_CONN_ID,
        sql=SqlQueries.user_table_insert,
        table='users',
        truncate=True,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        conn_id=REDSHIFT_CONN_ID,
        sql=SqlQueries.song_table_insert,
        table='songs',
        truncate=True,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        conn_id=REDSHIFT_CONN_ID,
        sql=SqlQueries.artist_table_insert,
        table='artists',
        truncate=True,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        conn_id=REDSHIFT_CONN_ID,
        sql=SqlQueries.time_table_insert,
        table='time',
        truncate=True,
    )

    tables = ['staging_events', 'staging_songs', 'songplays', 'users', 'songs', 'artists', 'time']
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        postgres_conn_id=REDSHIFT_CONN_ID,
        tests=[DataQualityTest.no_results_test(table) for table in tables]
    )

    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()