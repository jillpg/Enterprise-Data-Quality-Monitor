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

# Define DAG
with DAG(
    'enterprise_data_quality_monitor',
    default_args=default_args,
    description='ELT Pipeline: Chaos Generation -> S3 -> Snowflake -> dbt -> Dashboard',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Task 1: Generate Data (Chaos Monkey)
    t1_ingestion = BashOperator(
        task_id='run_ingestion',
        bash_command='python main.py',
        cwd='/opt/airflow/project'
    )

    # Task 2: Load to Snowflake
    t2_load = BashOperator(
        task_id='load_to_snowflake',
        bash_command='python src/snowflake_loader.py --mode incremental',
        cwd='/opt/airflow/project'
    )

    # Task 3: dbt Transformation (Run + Test)
    t3_transform = BashOperator(
        task_id='dbt_transform',
        bash_command='dbt run --profiles-dir . && dbt test --profiles-dir .',
        cwd='/opt/airflow/project/dbt_project'
    )

    # Task 4: Generate Dashboard Snapshot
    t4_snapshot = BashOperator(
        task_id='refresh_snapshot',
        bash_command='python src/snapshot_generator.py',
        cwd='/opt/airflow/project'
    )

    # Dependencies
    t1_ingestion >> t2_load >> t3_transform >> t4_snapshot
