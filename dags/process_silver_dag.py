from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
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

def run_stored_procedure(procedure_name, **kwargs):
    """
    Executes a stored procedure using PostgresHook.
    """
    hook = PostgresHook(postgres_conn_id='postgres_default')
    sql = f"CALL {procedure_name}(FALSE);"
    hook.run(sql)
    print(f"Executed: {sql}")

with DAG(
    'process_silver_data',
    default_args=default_args,
    description='Process Bronze data to Silver using Stored Procedures',
    schedule=None,
    catchup=False,
    tags=['silver', 'processing'],
) as dag:

    process_customer = PythonOperator(
        task_id='process_customer_silver',
        python_callable=run_stored_procedure,
        op_kwargs={'procedure_name': 'sp_process_silver_customer'}
    )

    process_store = PythonOperator(
        task_id='process_store_silver',
        python_callable=run_stored_procedure,
        op_kwargs={'procedure_name': 'sp_process_silver_store'}
    )

    process_menu = PythonOperator(
        task_id='process_silver_menu',
        python_callable=run_stored_procedure,
        op_kwargs={'procedure_name': 'sp_process_silver_menu'}
    )

    [process_customer, process_store, process_menu]

