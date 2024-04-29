from datetime import datetime, timedelta

from airflow import DAG
from airflow.operator.bash import BashOperator

default_args = {
    'owner': 'endiesworld',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


with DAG(
    dag_id='first_dag',
    default_args=default_args,
    description='This is my first dag on airflow',
    start_date=datetime(2024, 4, 19, 1),
    schedular_interval='@daily'
) as dag:
    task1 = BashOperator(
        task1='first_task',
        bash_command='echo Hello World, this is the first task on airflow'
    )
    task1