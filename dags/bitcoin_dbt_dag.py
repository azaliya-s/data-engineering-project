from datetime import datetime
from airflow import DAG
from plugins.operators.dbt_operator import DbtRunOperator, DbtTestOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 13),
}

with DAG(
    dag_id='bitcoin_dbt_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    run_bitcoin = DbtRunOperator(
        task_id='dbt_run_bitcoin',
        models='bitcoin_model',
        project_dir='/usr/local/airflow/dbt/bitcoin_project',
        profiles_dir='/usr/local/airflow/dbt'
    )

    test_bitcoin = DbtTestOperator(
        task_id='dbt_test_bitcoin',
        project_dir='/usr/local/airflow/dbt/bitcoin_project',
        profiles_dir='/usr/local/airflow/dbt'
    )

    run_bitcoin >> test_bitcoin

