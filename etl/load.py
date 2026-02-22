import logging
import polars as pl
from google.cloud import bigquery
from config import (
    GOOGLE_CLOUD_CREDENTIALS, DATASET_ID, WRITE_DISPOSITION,
    TRANSFORMED_FILEPATHS, MOVIES_DETAILS_TABLE_ID, RUNTIME_DISTRIBUTION_TABLE_ID,
    YEARLY_AGGREGATES_TABLE_ID, YEAR_GENRE_AGGREGATES_TABLE_ID
)
from utils.bigquery import load_df_to_bigquery, create_dataset
from utils.google_cloud_client import get_bigquery_client

def load_to_bigquery(
    df: pl.DataFrame, 
    project_id: str, 
    dataset_id: str, 
    table_id: str,
    google_cloud_client: bigquery.Client,
) -> None:
    """
    Loads transformed data into BigQuery.
    
    Args:
        df (pl.DataFrame): Transformed data to load
        project_id (str): Google Cloud project ID
        dataset_id (str): BigQuery dataset ID
        table_id (str): BigQuery table ID
        google_cloud_client (bigquery.Client): Authenticated BigQuery client instance
    
    Returns:
        None
    """
    try:
        # Create dataset if it doesn't exist
        create_dataset(google_cloud_client, dataset_id)
        
        # Construct full table ID
        full_table_id = f"{project_id}.{dataset_id}.{table_id}"
        
        # Load data to BigQuery
        logging.info(f"Loading {len(df)} rows to {full_table_id}")
        load_df_to_bigquery(df, full_table_id, google_cloud_client, WRITE_DISPOSITION)
        logging.info(f"Successfully loaded data to {full_table_id}")
        
    except Exception as e:
        logging.error(f"Error loading data to BigQuery: {e}")
        raise

def load_data(data_dict: dict[str, pl.DataFrame]) -> None:
    """
    Loads multiple DataFrames to different BigQuery tables.
    
    Args:
        data_dict (dict): Dictionary with table names as keys and DataFrames as values
    
    Returns:
        None
    """
    project_id = GOOGLE_CLOUD_CREDENTIALS.get("project_id")
    for table_name, df in data_dict.items():
        google_cloud_client = get_bigquery_client(GOOGLE_CLOUD_CREDENTIALS)
        load_to_bigquery(df, project_id, DATASET_ID, table_name, google_cloud_client)

if __name__ == "__main__":
    import logging

    logging.info("Starting data load to BigQuery...")
    try:
        movies_detailed_df = pl.read_csv(TRANSFORMED_FILEPATHS["MOVIES_DETAILED"])
        runtime_distribution_df = pl.read_csv(TRANSFORMED_FILEPATHS["RUNTIME_DISTRIBUTION"])
        yearly_aggregates_df = pl.read_csv(TRANSFORMED_FILEPATHS["YEARLY_AGGREGATES"])
        year_genre_aggregates_df = pl.read_csv(TRANSFORMED_FILEPATHS["YEAR_GENRE_AGGREGATES"])

        data_to_load = {
            MOVIES_DETAILS_TABLE_ID: movies_detailed_df,
            RUNTIME_DISTRIBUTION_TABLE_ID: runtime_distribution_df,
            YEARLY_AGGREGATES_TABLE_ID: yearly_aggregates_df,
            YEAR_GENRE_AGGREGATES_TABLE_ID: year_genre_aggregates_df,
        }
        load_data(data_to_load)
        
        logging.info("Data load to BigQuery completed successfully.")
    except Exception as e:
        logging.error(f"Data load to BigQuery failed: {str(e)}")


