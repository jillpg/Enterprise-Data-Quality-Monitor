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
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG: INITIALIZATION (Manual Trigger Only)
with DAG(
    'init_backfill_project',
    default_args=default_args,
    description='ONE-TIME INIT: Cleans S3, generates 12 months history, loads and refreshes everything.',
    schedule_interval=None, # Manual trigger only!
    catchup=False,
) as dag:

    # Task 1: Initialization Logic (Backfill 12 Months)
    # This runs src/init_project.py which cleans S3 and generates history
    t1_init_backfill = BashOperator(
        task_id='run_initialization_backfill',
        bash_command='python src/init_project.py',
        cwd='/opt/airflow/project'
    )

    # --- REUSED TASKS (Same as Daily Pipeline) ---

    # Task 2: Load to Snowflake (Standard Loader works for backfill too)
    t2_load = BashOperator(
        task_id='load_to_snowflake',
        bash_command='python src/snowflake_loader.py --mode full',
        cwd='/opt/airflow/project'
    )

    # Task 3: dbt Transformation (Full Run)
    t3_transform = BashOperator(
        task_id='dbt_transform',
        bash_command='dbt clean && dbt deps --profiles-dir . && dbt run --profiles-dir . && dbt test --profiles-dir .',
        cwd='/opt/airflow/project/dbt_project'
    )

    # Task 4: Generate Dashboard Snapshot
    t4_snapshot = BashOperator(
        task_id='refresh_snapshot',
        bash_command='python src/snapshot_generator.py',
        cwd='/opt/airflow/project'
    )

    # Dependencies
    t1_init_backfill >> t2_load >> t3_transform >> t4_snapshot
