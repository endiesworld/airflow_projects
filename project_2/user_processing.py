from airflow import DAG
from airflow.providers.postres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG('user_processing', start_date=datetime(2024, 1, 1), 
         schedule_interval='@daily', catchup=False) as dag:
     
    create_table = PostgresOperator(
         task_id='create_table', 
         postgres_conn_id='postgres',
         sql="""
               CREATE TABLE IF NOT EXISTS users (
                         firstname TEXT NOT NULL, 
                         lastname  TEXT NOT NULL,
                         country TEXT NOT NULL,
                         username TEXT NOT NULL,
                         password TEXT NOT NULL,
                         email TEXT NOT NULL
               );
         """
          )