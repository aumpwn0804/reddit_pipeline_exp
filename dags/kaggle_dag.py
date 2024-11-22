from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.kaggle_pipeline import kaggle_pipeline

# Define the Airflow DAG
default_args = {
    'owner': 'Aum',
    'start_date': datetime(2024, 1, 1)
}

dag = DAG(
    dag_id='etl_kaggle_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['kaggle', 'etl', 'pipeline']
)

run_kaggle_pipeline = PythonOperator(
    task_id='run_kaggle_pipeline',
    python_callable=kaggle_pipeline,
    dag=dag
)

run_kaggle_pipeline