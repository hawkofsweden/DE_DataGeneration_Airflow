from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

# Default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'process_silver_data',
    default_args=default_args,
    description='Process Bronze data to Silver using Stored Procedures',
    schedule=None, # Triggered manually or by dataset/external trigger
    catchup=False,
    tags=['silver', 'processing'],
) as dag:

    # Trigger Customer Processing
    process_customer = PostgresOperator(
        task_id='process_customer_silver',
        postgres_conn_id='postgres_default',
        sql="CALL sp_process_silver_customer(FALSE);",
        autocommit=True
    )

    # Trigger Store Processing
    process_store = PostgresOperator(
        task_id='process_store_silver',
        postgres_conn_id='postgres_default',
        sql="CALL sp_process_silver_store(FALSE);",
        autocommit=True
    )

    # Trigger Menu Processing
    process_menu = PostgresOperator(
        task_id='process_menu_silver',
        postgres_conn_id='postgres_default',
        sql="CALL sp_process_silver_menu(FALSE);",
        autocommit=True
    )

    # Parallel Execution
    [process_customer, process_store, process_menu]
