
    # run_bitcoin = DbtRunOperator(
    #     task_id='dbt_run_bitcoin',
    #     models='bitcoin_model',
    #     project_dir='/usr/local/airflow/dbt/bitcoin_project',
    #     profiles_dir='/usr/local/airflow/dbt'
    # )

    # test_bitcoin = DbtTestOperator(
    #     task_id='dbt_test_bitcoin',
    #     project_dir='/usr/local/airflow/dbt/bitcoin_project',
    #     profiles_dir='/usr/local/airflow/dbt'
    # )

    # run_bitcoin >> test_bitcoin

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

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

    # Задача для проверки доступности dbt, например, dbt debug
    check_dbt = BashOperator(
         task_id='check_dbt',
         bash_command='dbt debug --profiles-dir /usr/local/airflow/dbt/bitcoin_project --project-dir /usr/local/airflow/dbt/bitcoin_project'
    )

    # Можно также запустить простую команду dbt --version или dbt debug без проекта
    # check_version = BashOperator(
    #     task_id='check_dbt_version',
    #     bash_command='dbt --version'
    # )

    # Например, потом можно пробовать dbt run (но это уже будет пытаться выполнить модель)
    # run_dbt = BashOperator(
    #     task_id='run_dbt_model',
    #     bash_command='dbt run --profiles-dir /usr/local/airflow/dbt --project-dir /usr/local/airflow/dbt/bitcoin_project'
    # )
    
    check_dbt
    
    
