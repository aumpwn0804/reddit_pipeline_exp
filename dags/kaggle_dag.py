from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from pipelines.kaggle_pipeline import kaggle_pipeline

# Define the Airflow DAG
with DAG(
    'kaggle_data_pipeline',
    default_args={'owner': 'airflow', 'start_date': datetime(2024, 1, 1)},
    schedule_interval='@monthly',
    catchup=False,
) as dag:

    run_pipeline = PythonOperator(
        task_id='run_kaggle_pipeline',
        python_callable=kaggle_pipeline
    )
