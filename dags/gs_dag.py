import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials



# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Initialize the DAG
with DAG(
    dag_id='data_to_google_sheets',
    default_args=default_args,
    description='A pipeline to deliver data to Google Sheets',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Function to extract data
    def extract_data():
        # Replace with your data source logic
        data = [
            {'Name': 'Alice', 'Age': 25, 'City': 'New York'},
            {'Name': 'Bob', 'Age': 30, 'City': 'San Francisco'},
        ]
        return pd.DataFrame(data)

    # Function to transform data
    def transform_data(ti):
        try:
            df = ti.xcom_pull(task_ids='extract_data_task')
            df['Age'] = df['Age']  # Example transformation
            key = pd.read_csv("/opt/airflow/config/gs-data-pipeline-442808-a9194f4d13d0.json")
            print(key)
            return df
        except Exception as e:
            print(e)
            sys.exit(1)

    # Function to load data into Google Sheets
    def load_to_google_sheets(ti):
        try:
            # Google Sheets authentication
            print("connecting")
            scope = ['https://www.googleapis.com/auth/spreadsheets']
            key = pd.read_csv("/opt/airflow/config/gs-data-pipeline-442808-a9194f4d13d0.json")
            print(key)
            creds = Credentials.from_service_account_file('/opt/airflow/config/gs-data-pipeline-442808-a9194f4d13d0.json', scopes=scope)
            client = gspread.authorize(creds)
            print(client)
            print("connect success")

            # Open Google Sheet
            print("opening")
            sheet_id = "1aJa-SBxYymXJvQ0vF5xf1je3MHckQwV1bZLob7s0RIc"
            workbook = client.open_by_key(sheet_id)
            sheet1 = workbook.worksheet("sheet1")
            print("open success")
            # Get transformed data
            df = ti.xcom_pull(task_ids='transform_data_task')
            df = pd.DataFrame(df)
            print(df)

            # Update Google Sheet
            sheet1.clear()
            sheet1.update([df.columns.values.tolist()] + df.values.tolist())
        except Exception as e:
            print(e)
            sys.exit(1)

    # Define tasks
    extract_data_task = PythonOperator(
        task_id='extract_data_task',
        python_callable=lambda: extract_data().to_dict(),
    )

    transform_data_task = PythonOperator(
        task_id='transform_data_task',
        python_callable=transform_data,
    )

    load_to_google_sheets_task = PythonOperator(
        task_id='load_to_google_sheets_task',
        python_callable=load_to_google_sheets,
    )

    # Set task dependencies
    extract_data_task >> transform_data_task >> load_to_google_sheets_task
