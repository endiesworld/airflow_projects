from datetime import datetime

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.operators.bash import BashOperator

my_file = Dataset(uri="/tmp/my_file.txt", extra=None)

# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="producer", start_date=datetime(2022, 1, 1), schedule="@daily",
         catchup=False) as dag:
    # Tasks are represented as operators
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    @task(outlets=[my_file])
    def update_dataset():
        with open(my_file.uri, "a+") as f:
            f.write("producer updated")

    # Set dependencies between tasks
    hello >> update_dataset()