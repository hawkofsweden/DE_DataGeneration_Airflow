from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

REPO_URL = "https://github.com/hawkofsweden/DE_DataGeneration_Airflow"
TARGET_DIR = "/opt/airflow/projects"

with DAG(
    dag_id='0_infrastructure_git_sync',
    schedule_interval='*/5 * * * *', # Checks every 5 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['infra']
) as dag:

    smart_pull = BashOperator(
        task_id='pull_repo',
        bash_command=f"""
            if [ ! -d "{TARGET_DIR}/.git" ]; then
                echo "Repo not found. Cloning..."
                git clone {REPO_URL} {TARGET_DIR}
            else
                cd {TARGET_DIR}
                git fetch origin main
                LOCAL=$(git rev-parse HEAD)
                REMOTE=$(git rev-parse @{{u}})
                if [ "$LOCAL" != "$REMOTE" ]; then
                    echo "New changes detected. Pulling..."
                    git pull origin main
                else
                    echo "No changes found. Skipping download."
                fi
            fi
        """
    )