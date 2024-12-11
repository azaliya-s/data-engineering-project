from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def say_hello():
    print("Hello from Airflow!")
    return "Hello completed"

with DAG(
    'simple_python_dag',
    description='A simple DAG with Python task',
    schedule='@once',  # Runs only once
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=say_hello
    )

    # Task flow
    hello_task