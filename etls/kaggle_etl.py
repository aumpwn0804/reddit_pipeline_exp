import os
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi

# Step 1: Authenticate and Initialize Kaggle API
api = KaggleApi()
api.authenticate()

# Step 2: Download the Kaggle Dataset
def download_dataset(dataset_name, file_name, download_path='./data'):
    """
    Downloads a dataset from Kaggle and saves it to the specified path.
    """
    os.makedirs(download_path, exist_ok=True)
    api.dataset_download_file(dataset_name, file_name, path=download_path)
    print(f"Downloaded {file_name} to {download_path}")

# Example: Replace these with your dataset and file
dataset_name = "karkavelrajaj/amazon-sales-dataset"  # Kaggle dataset identifier
file_name = "Amazon Sale Report.csv"  # Specific file in the dataset
download_dataset(dataset_name, file_name)

# Step 3: Extract and Load Data
def load_data(file_path):
    """
    Reads the CSV file into a pandas DataFrame.
    """
    csv_path = file_path.replace('.zip', '')
    df = pd.read_csv(csv_path)
    print(f"Data Loaded: {len(df)} rows")
    return df

data_path = f"./data/{file_name}"
df = load_data(data_path)

# Step 4: Process Data
def process_data(df):
    """
    Perform basic data cleaning and processing.
    """
    # Example: Rename columns
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]

    # Drop duplicates
    df = df.drop_duplicates()

    # Fill missing values
    df = df.fillna(method='ffill')
    
    print("Data Processing Completed")
    return df

df_processed = process_data(df)

# Step 5: Save or Upload Processed Data
def save_data(df, output_path='./processed_data/processed.csv'):
    """
    Save the processed data locally.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Processed Data Saved to {output_path}")

save_data(df_processed)
