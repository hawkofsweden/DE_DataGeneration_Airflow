from airflow import DAG
from airflow.operators.bash import BashOperator
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
    'infrastructure_git_sync',
    default_args=default_args,
    description='Sync dags and scripts from GitHub',
    schedule=timedelta(minutes=10),
    catchup=False,
    tags=['infrastructure', 'git'],
) as dag:

    # Script to sync repo:
    # 1. Clone if not exists.
    # 2. If exists, fetch and reset hard to origin/main (destroys local changes to ensure sync).
    sync_command = """
        TARGET_DIR="/opt/airflow/projects"
        REPO_URL="https://github.com/hawkofsweden/DE_DataGeneration_Airflow"
        
        if [ ! -d "$TARGET_DIR/.git" ]; then
            echo "Repo not found or not a git repo. Cloning..."
            rm -rf "$TARGET_DIR"  # Clean up if partial
            git clone "$REPO_URL" "$TARGET_DIR"
        else
            echo "Repo exists. Syncing..."
            cd "$TARGET_DIR"
            git fetch origin main
            # Force reset to match remote, discarding local changes (like .DS_Store or accidental edits)
            git reset --hard origin/main
            git clean -fd  # Remove untracked files
        fi
        
        # Deploy DAGs to Airflow DAGs directory
        SOURCE_DAGS="$TARGET_DIR/dags"
        DEST_DAGS="/opt/airflow/dags"
        
        echo "Checking source directory: $SOURCE_DAGS"
        ls -la "$SOURCE_DAGS"
        
        if [ -d "$SOURCE_DAGS" ]; then
            echo "Deploying DAGs from $SOURCE_DAGS to $DEST_DAGS..."
            # Use cp -r with . to catch all files including hidden ones, and -v for verbose
            cp -Rv "$SOURCE_DAGS"/* "$DEST_DAGS/" || echo "Copy warning: Check if destination is writable or files match."
            echo "Listing destination $DEST_DAGS after copy:"
            ls -la "$DEST_DAGS"
            echo "DAGs deployed successfully."
        else
            echo "WARNING: $SOURCE_DAGS does not exist. Skipping deployment."
        fi
    """

    pull_repo = BashOperator(
        task_id='pull_repo',
        bash_command=sync_command,
    )
