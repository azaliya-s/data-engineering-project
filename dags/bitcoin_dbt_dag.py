from airflow import DAG
from airflow.providers.bash.operators.bash import BashOperator
from datetime import datetime

# Инициализация DAG
dag = DAG(
    'bitcoin_dbt_dag',
    description='Run DBT Models for Bitcoin Data',
    schedule_interval=None,  
    start_date=datetime(2024, 12, 12),
    catchup=False
)

# Оператор для выполнения dbt run
dbt_run = BashOperator(
    task_id='run_dbt_models',
    bash_command='dbt run --profiles-dir ./dbt/bitcoin_project/profiles.yml --project-dir ./dbt/bitcoin_project',
    dag=dag
)

dbt_run
