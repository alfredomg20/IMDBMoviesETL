import polars as pl
from adbc_driver_postgresql import dbapi

def load_df_to_postgresql(df: pl.DataFrame, table_name: str, uri: str, engine: str, write_disposition: str = "truncate") -> None:
    """ Loads a Polars DataFrame directly into a PostgreSQL table"""

    if write_disposition.lower() == "truncate":
        with dbapi.connect(uri, engine=engine) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"TRUNCATE TABLE IF EXISTS {table_name} RESTART IDENTITY CASCADE;")
        if_table_exists = "append"
    elif write_disposition.lower() == "replace":
        if_table_exists = "replace"
    elif write_disposition.lower() == "append":
        if_table_exists = "append"
    else:
        raise ValueError(f"Unsupported write_disposition: '{write_disposition}'. Use 'truncate', 'replace', or 'append'.")

    df.write_database(
        table_name=table_name,
        connection=uri,
        engine=engine,
        if_table_exists=if_table_exists
    )