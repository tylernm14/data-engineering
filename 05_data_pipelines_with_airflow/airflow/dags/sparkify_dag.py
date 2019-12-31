from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator,LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from airflow.operators.subdag_operator import SubDagOperator
from helpers import SqlQueries
from dim_subdag import load_dim_table_dag


default_args = {
    'owner': 'tyler',
    'depends_on_past': False,
    'catchup_by_default': False,
    'start_date': datetime.utcnow(),
    'retires': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag_name = 'sparkify_dag'
start_date=datetime.utcnow()
dag = DAG(dag_name,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          start_date=start_date,
          max_active_runs=4
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json="log_json_path.json",
    region='us-west-2',
    sql_create = SqlQueries.staging_events_table_create
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region='us-west-2',
    sql_create = SqlQueries.staging_songs_table_create
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='songplays',
    sql_create=SqlQueries.songplay_table_create,
    drop_first=True,
    sql_insert=SqlQueries.songplay_table_insert
)

load_user_dimension_table = SubDagOperator(
    subdag=load_dim_table_dag(
        parent_dag=dag_name,
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='users',
        sql_create=SqlQueries.user_table_create,
        sql_insert=SqlQueries.user_table_insert,
        start_date=start_date
    ),
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = SubDagOperator(
    subdag=load_dim_table_dag(
        parent_dag=dag_name,
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='songs',
        sql_create=SqlQueries.song_table_create,
        sql_insert=SqlQueries.song_table_insert,
        start_date=start_date
    ),
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = SubDagOperator(
    subdag=load_dim_table_dag(
        parent_dag=dag_name,
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='artists',
        sql_create=SqlQueries.artist_table_create,
        sql_insert=SqlQueries.artist_table_insert,
        start_date=start_date
    ),
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = SubDagOperator(
    subdag=load_dim_table_dag(
        parent_dag=dag_name,
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='songs',
        sql_create=SqlQueries.time_table_create,
        sql_insert=SqlQueries.time_table_insert,
        start_date=start_date
    ),
    task_id='Load_time_dim_table',
    dag=dag
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    sql_checks_and_expects = SqlQueries.checks_and_expects
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, load_song_dimension_table,
                         load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator