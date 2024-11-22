from etls.kaggle_etl import download_dataset,load_data,process_data,save_data

def kaggle_pipeline(dataset_name, file_name, data_path):
    # Reuse the functions from the script above
    download_dataset(dataset_name, file_name)
    df = load_data(data_path)
    df_processed = process_data(df)
    save_data(df_processed)