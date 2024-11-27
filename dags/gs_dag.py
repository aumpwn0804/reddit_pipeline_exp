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
    'owner': 'Aum',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Initialize the DAG
dag = DAG(
    dag_id='data_to_google_sheets',
    default_args=default_args,
    description='A pipeline to deliver data to Google Sheets',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

    # Function to extract data
def extract_data():
        # Replace with your data source logic
    data = [
    {
        "Name": "Alice",
        "Details": {"Age": 25, "City": "New York"},
        "Hobbies": [
            {"Hobby": "Reading", "SkillLevel": "Intermediate"},
            {"Hobby": "Cycling", "SkillLevel": "Beginner"}
        ],
        "Jobs": [
            {"Title": "Engineer", "Company": "ABC Corp"},
            {"Title": "Writer", "Company": "XYZ Inc"}
        ]
    },
    {
        "Name": "Bob",
        "Details": {"Age": 30, "City": "San Francisco"},
        "Hobbies": [
            {"Hobby": "Cooking", "SkillLevel": "Advanced"},
            {"Hobby": "Swimming", "SkillLevel": "Intermediate"}
        ],
        "Jobs": [
            {"Title": "Chef", "Company": "Gourmet Inc"},
            {"Title": "Diver", "Company": "Ocean Co"}
        ]
    }
    ]
    df_hobbies = pd.json_normalize(
    data,
    record_path='Hobbies',
    meta=['Name', ['Details', 'Age'], ['Details', 'City'], 'Jobs'],
    sep="_"
)

    # Explode the second object array (Jobs) to handle the cartesian product
    df_hobbies['Jobs'] = df_hobbies['Jobs'].apply(lambda x: pd.json_normalize(x))
    df_final = df_hobbies.explode('Jobs')

    # Normalize the exploded 'Jobs' field and merge it back
    df_jobs = pd.json_normalize(df_final['Jobs'])
    df_final = pd.concat([df_final.drop(columns=['Jobs']).reset_index(drop=True), df_jobs], axis=1)

    print("Flattened DataFrame:")
    print(df_final)
    return df_final

    # Function to transform data
def transform_data(ti):
        try:
            df = ti.xcom_pull(task_ids='extract_data_task')
            df['Name'] = df['Name']  # Example transformation
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
        dag=dag
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
