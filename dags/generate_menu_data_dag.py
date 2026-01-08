from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

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
    'generate_menu_data',
    default_args=default_args,
    description='Generate Menu Data',
    schedule=None,
    catchup=False,
    tags=['data_generation'],
) as dag:

    generate_task = BashOperator(
        task_id='generate_menu_data_task',
        bash_command='python /opt/airflow/scripts/menu_data.py',
    )
