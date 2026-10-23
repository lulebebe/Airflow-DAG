from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("Hola CI/CD")
    return "OK"

with DAG(
    dag_id='example_ci_cd_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['example', 'ci-cd']
) as dag:

    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello
    )
