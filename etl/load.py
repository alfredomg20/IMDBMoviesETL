import logging
import polars as pl
from config import (
    GOOGLE_CLOUD_CREDENTIALS, BIGQUERY_CONF,
    POSTGRES_CONF, TRANSFORMED_FILEPATHS,
)
from utils.bigquery import load_df_to_bigquery, create_dataset
from utils.google_cloud_client import get_bigquery_client
from utils.postgresql import load_df_to_postgresql

def load_data(data_dict: dict[str, pl.DataFrame]) -> None:
    """ Loads transformed data to BigQuery and PostgreSQL based on configuration. """

    # Init BigQuery client and create dataset if it doesn't exist
    bg_client = get_bigquery_client(GOOGLE_CLOUD_CREDENTIALS)
    create_dataset(bg_client, BIGQUERY_CONF["dataset_id"])

    # Get values from config dicts
    project_id = GOOGLE_CLOUD_CREDENTIALS.get("project_id")
    dataset_id = BIGQUERY_CONF["dataset_id"]
    bg_tables = BIGQUERY_CONF["table_ids"]
    bg_write_disposition = BIGQUERY_CONF["write_disposition"]
    
    pg_uri = POSTGRES_CONF["uri"]
    pg_database = POSTGRES_CONF["database"]
    pg_tables = POSTGRES_CONF["table_names"]
    pg_driver = POSTGRES_CONF["driver"]
    pg_write_disposition = POSTGRES_CONF["write_disposition"]
    
    for table, df in data_dict.items():
        try:
            if table in bg_tables:
                table_name = bg_tables[table]
                full_table_id = f"{project_id}.{dataset_id}.{table_name}"
                logging.info(f"Loading {len(df)} records to BigQuery -> {full_table_id}")
                load_df_to_bigquery(df, full_table_id, bg_client, write_disposition=bg_write_disposition)
            if table in pg_tables:
                table_name = pg_tables[table]
                full_table_name = f"{pg_database}.{table_name}"
                logging.info(f"Loading {len(df)} records to PostgreSQL -> {full_table_name}")
                load_df_to_postgresql(df, full_table_name, pg_uri, pg_driver, write_disposition=pg_write_disposition)
            else:
                logging.warning(f"Table {table} not found in configuration dictionaries. Skipping load for this table.")
        except Exception as e:
            logging.error(f"Failed to load data for table {table}: {str(e)}")
            raise e

if __name__ == "__main__":
    import logging
    from config import DATA_SCHEMA

    logging.info("Starting data load to BigQuery...")
    try:
        movies_detailed_df = pl.read_csv(TRANSFORMED_FILEPATHS["MOVIES_DETAILED"], schema_overrides=DATA_SCHEMA["movies_detailed"])
        runtime_distribution_df = pl.read_csv(TRANSFORMED_FILEPATHS["RUNTIME_DISTRIBUTION"], schema_overrides=DATA_SCHEMA["runtime_distribution"])
        yearly_aggregates_df = pl.read_csv(TRANSFORMED_FILEPATHS["YEARLY_AGGREGATES"], schema_overrides=DATA_SCHEMA["yearly_aggregates"])
        year_genre_aggregates_df = pl.read_csv(TRANSFORMED_FILEPATHS["YEAR_GENRE_AGGREGATES"], schema_overrides=DATA_SCHEMA["year_genre_aggregates"])

        data_to_load = {
            "movies_detailed": movies_detailed_df,
            "runtime_distribution": runtime_distribution_df,
            "yearly_aggregates": yearly_aggregates_df,
            "year_genre_aggregates": year_genre_aggregates_df,
        }
        load_data(data_to_load)
        
        logging.info("Data load completed.")
    except Exception as e:
        logging.error(f"Data load failed: {str(e)}")
