from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def do_something():
    print("Doing something")

with DAG(
    dag_id='bad_cyclic_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['ci-cd', 'bad-example']
) as dag:

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=do_something
    )
    
    task_b = PythonOperator(
        task_id='task_b',
        python_callable=do_something
    )

    task_a >> task_b
