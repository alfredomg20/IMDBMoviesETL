import polars as pl

def apply_polars_schema(df: pl.DataFrame, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    """Casts DataFrame columns to match the target schema."""
    return df.with_columns([
        pl.col(col).cast(dtype)
        for col, dtype in schema.items()
        if col in df.columns
    ])
