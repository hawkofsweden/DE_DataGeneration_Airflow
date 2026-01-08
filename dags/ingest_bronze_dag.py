from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import os
import glob
import pandas as pd
from io import StringIO

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

def ingest_data_to_bronze(table_name, file_prefix, **kwargs):
    """
    Ingests the latest CSV file for a given prefix into the bronze table.
    Adds 'file_name' column to the data before ingestion.
    """
    base_dir = "/opt/airflow/data_output"
    search_pattern = os.path.join(base_dir, f"{file_prefix}*.csv")
    
    # Create archive directory if it doesn't exist
    archive_dir = os.path.join(base_dir, "archive")
    os.makedirs(archive_dir, exist_ok=True)

    # Find all files matching the pattern
    files = glob.glob(search_pattern)
    if not files:
        print(f"No files found for pattern: {search_pattern}")
        return

    # Sort files by creation time for FIFO processing
    files.sort(key=os.path.getctime)
    
    print(f"Found {len(files)} files to process. Starting batch ingestion...")

    for target_file in files:
        file_basename = os.path.basename(target_file)
        print(f"Processing file: {target_file} into {table_name}")

        # Read CSV using pandas
        try:
            df = pd.read_csv(target_file)
        except Exception as e:
            print(f"Error reading CSV {target_file}: {e}")
            # If one fails, we might want to continue or stop. 
            # Stopping is safer to preserve FIFO order integrity if dependence exists.
            raise

        # Add file_name column
        df['file_name'] = file_basename

        # PostgreSQL COPY requires a file-like object
        buffer = StringIO()
        
        columns = list(df.columns)
        column_str = ",".join(columns)
        
        df.to_csv(buffer, index=False, header=True)
        buffer.seek(0)

        # Use COPY with CSV HEADER to handle the buffer with header
        sql = f"COPY {table_name} ({column_str}) FROM STDIN WITH CSV HEADER"
        
        hook = PostgresHook(postgres_conn_id='postgres_default')
        
        try:
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.copy_expert(sql, buffer)
            print(f"Successfully ingested {target_file} into {table_name} with {len(df)} rows.")
            
            # Archive the file after successful ingestion
            archive_path = os.path.join(archive_dir, file_basename)
            import shutil
            shutil.move(target_file, archive_path)
            print(f"Moved processed file to archive: {archive_path}")

        except Exception as e:
            print(f"Error executing COPY command or moving file: {e}")
            raise
            
    print("Batch ingestion completed.")

with DAG(
    'ingest_to_bronze',
    default_args=default_args,
    description='Ingest generated CSVs to Bronze Postgres tables',
    schedule=None,
    catchup=False,
    tags=['bronze', 'ingestion'],
) as dag:



    ingest_customer = PythonOperator(
        task_id='ingest_customer',
        python_callable=ingest_data_to_bronze,
        op_kwargs={
            'table_name': 'bronze_customer',
            'file_prefix': 'customer_data_'
        }
    )

    ingest_store = PythonOperator(
        task_id='ingest_store',
        python_callable=ingest_data_to_bronze,
        op_kwargs={
            'table_name': 'bronze_store',
            'file_prefix': 'store_data_'
        }
    )

    ingest_menu = PythonOperator(
        task_id='ingest_menu',
        python_callable=ingest_data_to_bronze,
        op_kwargs={
            'table_name': 'bronze_menu',
            'file_prefix': 'menu_data_'
        }
    )

    [ingest_customer, ingest_store, ingest_menu]
