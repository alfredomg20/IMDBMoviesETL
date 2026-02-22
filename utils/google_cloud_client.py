from google.oauth2 import service_account
from google.cloud import bigquery

def get_bigquery_client(credentials_dict: dict[str, str]) -> bigquery.Client:
    """
    Creates and returns a BigQuery client using the provided service account credentials as a dict.

    Args:
        credentials_dict (dict[str, str]): Dictionary containing service account credentials.

    Returns:
        bigquery.Client: An authenticated BigQuery client instance.
    """
    try:
        if not credentials_dict:
            raise ValueError("Failed to load Google Cloud credentials: credentials dictionary is empty.")
        if not credentials_dict.get("project_id"):
            raise ValueError("Failed to load Google Cloud credentials: 'project_id' is missing in credentials dictionary.")
        
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        client = bigquery.Client(credentials=credentials, project=credentials_dict["project_id"])
        
        return client
    except Exception as e:
        raise RuntimeError(f"Failed to create BigQuery client: {e}")