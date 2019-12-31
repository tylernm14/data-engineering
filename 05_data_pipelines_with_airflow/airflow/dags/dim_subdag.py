from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import LoadDimensionOperator
from helpers import SqlQueries

def load_dim_table_dag(
    parent_dag,
    task_id,
    redshift_conn_id,
    aws_credentials_id,
    table,
    sql_create="",
    drop_first=False,
    sql_insert="",
    truncate=True,
    *args, **kwargs):
    """
    Returns a dag that inserts into a dimension table.
    Optionally creates table first and truncates it
    """
   
    dag = DAG(f"{parent_dag}.{task_id}", **kwargs) 
    
    load_dim_table = LoadDimensionOperator(
        task_id=f"Load_{table}_dim_table",
        dag=dag,
        table=table,
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        sql_create=sql_create,
        drop_first=drop_first,
        sql_insert=sql_insert,
        truncate=truncate
    )
    
    # Dependencies
    load_dim_table
    
    return dag
        
    