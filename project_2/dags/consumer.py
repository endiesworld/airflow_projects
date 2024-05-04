from datetime import datetime

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.operators.bash import BashOperator

my_file = Dataset("/tmp/my_file.txt")
my_file_2 = Dataset("/tmp/my_file_2.txt")

# A DAG represents a workflow, a collection of tasks
with DAG(
    dag_id="consumer", 
    start_date=datetime(2022, 1, 1), 
    schedule=[my_file, my_file_2],
    catchup=False) as dag:
    # Tasks are represented as operators
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    @task()
    def read_dataset():
        with open(my_file.uri, "r") as f:
            print(f.read())

    # Set dependencies between tasks
    hello >> read_dataset()