import os
import logging
from datetime import datetime
from dotenv import load_dotenv
import polars as pl

# Load environment variables from .env file
load_dotenv()

# General configuration
DEBUG = os.getenv("DEBUG", "true").lower() == "true"
CURRENT_YEAR = datetime.now().year

# Source Configuration
IMDB_BASE_URL = "https://datasets.imdbws.com/"

RAW_FILE_NAMES = {
    "MOVIES": "title.basics.tsv.gz",
    "RATINGS": "title.ratings.tsv.gz",
}

# Local Storage Configuration
RAW_DIR = "raw_data/"
TRANSFORMED_DIR = "transformed_data/"

RAW_FILEPATHS = {
    "MOVIES": os.path.join(RAW_DIR, RAW_FILE_NAMES["MOVIES"]),
    "RATINGS": os.path.join(RAW_DIR, RAW_FILE_NAMES["RATINGS"]),
}

TRANSFORMED_FORMAT = "csv"
TRANSFORMED_FILEPATHS = {
    "MOVIES_DETAILED": os.path.join(TRANSFORMED_DIR, f"movies_detailed.{TRANSFORMED_FORMAT}"),
    "YEARLY_AGGREGATES": os.path.join(TRANSFORMED_DIR, f"yearly_aggregates.{TRANSFORMED_FORMAT}"),
    "YEAR_GENRE_AGGREGATES": os.path.join(TRANSFORMED_DIR, f"year_genre_aggregates.{TRANSFORMED_FORMAT}"),
    "RUNTIME_DISTRIBUTION": os.path.join(TRANSFORMED_DIR, f"runtime_distribution.{TRANSFORMED_FORMAT}"),
}

# Google Cloud configuration
GOOGLE_CLOUD_CREDENTIALS = {
    "type": os.getenv("TYPE"),
    "project_id": os.getenv("PROJECT_ID"),
    "private_key_id": os.getenv("PRIVATE_KEY_ID"),
    "private_key": os.getenv("PRIVATE_KEY"),
    "client_email": os.getenv("CLIENT_EMAIL"),
    "client_id": os.getenv("CLIENT_ID"),
    "auth_uri": os.getenv("AUTH_URI"),
    "token_uri": os.getenv("TOKEN_URI"),
    "auth_provider_x509_cert_url": os.getenv("AUTH_PROVIDER_X509_CERT_URL"),
    "client_x509_cert_url": os.getenv("CLIENT_X509_CERT_URL"),
    "universe_domain": os.getenv("UNIVERSE_DOMAIN"),
}

# BigQuery configuration
BIGQUERY_CONF = {
    "dataset_id": os.getenv("DATASET_ID"),
    "table_ids": {
        "movies_detailed": os.getenv("MOVIES_DETAILS_TABLE", "movies_detailed"),
    },
    "write_disposition": os.getenv("BIGQUERY_WRITE_DISPOSITION", "WRITE_TRUNCATE"),
}

# PostgreSQL configuration
POSTGRES_CONF = {
    "uri": os.getenv("POSTGRES_URI"),
    "database": os.getenv("POSTGRES_DATABASE"),
    "table_names": {
        "movies_detailed": os.getenv("MOVIES_DETAILS_TABLE", "movies_detailed"),
        "runtime_distribution": os.getenv("RUNTIME_DISTRIBUTION_TABLE", "runtime_distribution"),
        "yearly_aggregates": os.getenv("YEARLY_AGGREGATES_TABLE", "yearly_aggregates"),
        "year_genre_aggregates": os.getenv("YEAR_GENRE_AGGREGATES_TABLE", "year_genre_aggregates")
    },
    "write_disposition": os.getenv("POSTGRES_WRITE_DISPOSITION", "append"),
    "driver": os.getenv("POSTGRES_DRIVER", "adbc")
}

# Schema definitions for destination tables
MOVIES_DETAILED_SCHEMA = {
    "movie_title": pl.String,
    "is_adult": pl.Int16,
    "runtime_minutes": pl.Int32,
    "genres": pl.String,
    "release_year": pl.Int16,
    "average_rating": pl.Float32,
    "total_votes": pl.Int32,
}

RUNTIME_DISTRIBUTION_SCHEMA = {
    "runtime_bin": pl.String,
    "total_movies": pl.Int32,
    "average_rating": pl.Float32,
    "min_runtime": pl.Int16,
    "max_runtime": pl.Int32,
}

YEARLY_AGGREGATES_SCHEMA = {
    "release_year": pl.Int16,
    "total_movies": pl.Int32,
    "average_rating": pl.Float32,
}

YEAR_GENRE_AGGREGATES_SCHEMA = {
    "release_year": pl.Int16,
    "genre": pl.String,
    "total_movies": pl.Int32,
    "average_rating": pl.Float32,
    "total_votes": pl.Int32,
}

DATA_SCHEMA = {
    "movies_detailed": MOVIES_DETAILED_SCHEMA,
    "runtime_distribution": RUNTIME_DISTRIBUTION_SCHEMA,
    "yearly_aggregates": YEARLY_AGGREGATES_SCHEMA,
    "year_genre_aggregates": YEAR_GENRE_AGGREGATES_SCHEMA,
}

# Configure logging for write to both file and console
logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Set encoding for StreamHandler
for handler in logging.getLogger().handlers:
    if isinstance(handler, logging.StreamHandler):
        handler.stream.reconfigure(encoding='utf-8')
