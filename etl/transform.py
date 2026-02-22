import polars as pl
from config import CURRENT_YEAR, DATA_SCHEMA
from utils.data_schema import apply_polars_schema

def prepare_movies(df: pl.DataFrame) -> pl.DataFrame:
    df = df.filter(pl.col("titleType") == "movie")
    df = df.with_columns(
        pl.when(pl.col('startYear').is_not_null())
        .then(pl.col('startYear'))
        .otherwise(pl.col('endYear'))
        .cast(pl.Int32)
        .alias('releaseYear')
    )
    columns_to_drop = ['titleType', 'startYear', 'endYear', 'originalTitle']
    df = df.drop(columns_to_drop)
    columns_to_rename = {'primaryTitle': 'movieTitle'}
    df = df.rename(columns_to_rename)
    return df

def create_movies_detailed(movies_df: pl.DataFrame, ratings_df: pl.DataFrame) -> pl.DataFrame:
    # Enrich movies_detailed with ratings data
    id_col = "tconst"
    movies_detailed_df = prepare_movies(movies_df)
    merged_df = movies_detailed_df.join(ratings_df, on=id_col, how="left")
    merged_df = merged_df.drop(id_col)
    
    original_columns = merged_df.columns
    renamed_columns = [pl.col(col).alias(
        ''.join(['_' + c.lower() if c.isupper() else c for c in col]).lstrip('_')
    ) for col in original_columns]
    merged_df = merged_df.select(renamed_columns)
    merged_df = merged_df.rename({"num_votes": "total_votes"})
    
    merged_df = apply_polars_schema(merged_df, DATA_SCHEMA["movies_detailed"])
    
    return merged_df

def create_runtime_distribution(movies_df: pl.DataFrame) -> pl.DataFrame:
    # Filter relevant data
    runtime_dist_df = movies_df.select([
        "runtime_minutes", "average_rating"
    ]).filter(pl.col("runtime_minutes").is_not_null())
    
    # Create runtime bins
    bins = [i for i in range(0, 241, 30)]
    labels = [f"< {bins[0]}"]  # First label for values below first bin
    labels.extend([f"{bins[i]+1}-{bins[i+1]}" for i in range(len(bins)-1)])  # Middle labels
    labels.append(f">{bins[-1]}")  # Last label for values >= last bin
    
    # Bin runtime_minutes and calculate aggregates
    runtime_dist_df = runtime_dist_df.with_columns(
        pl.col("runtime_minutes")
        .cut(bins, labels=labels, include_breaks=False)
        .alias("runtime_bin")
        .cast(pl.String)
    ).group_by("runtime_bin").agg([
        pl.len().alias("total_movies"),
        pl.mean("average_rating").round(2).alias("average_rating"),
        pl.min("runtime_minutes").alias("min_runtime"),
        pl.max("runtime_minutes").alias("max_runtime")
    ]).sort("min_runtime")

    runtime_dist_df = apply_polars_schema(runtime_dist_df, DATA_SCHEMA["runtime_distribution"])

    return runtime_dist_df

def create_yearly_aggregates(movies_df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    movies_year_df = movies_df.select([
        "release_year", "average_rating", "genres", "total_votes"
    ]).filter(
        pl.col("release_year").is_not_null() & (pl.col("release_year") <= CURRENT_YEAR)
    )

    yearly_aggregates_df = movies_year_df.group_by("release_year").agg([
        pl.len().alias("total_movies"),
        pl.mean("average_rating").round(2).alias("average_rating")
    ]).sort("release_year")
    
    yearly_aggregates_df = apply_polars_schema(yearly_aggregates_df, DATA_SCHEMA["yearly_aggregates"])

    return yearly_aggregates_df, movies_year_df

def create_year_genre_aggregates(movies_df: pl.DataFrame) -> pl.DataFrame:
    year_genre_df = movies_df.filter(pl.col("genres").is_not_null())
    year_genre_df = year_genre_df.with_columns(
        pl.col("genres").str.split(",").alias("genre")
    ).explode("genre")
    year_genre_df = year_genre_df.group_by(["release_year", "genre"]).agg([
        pl.len().alias("total_movies"),
        pl.mean("average_rating").round(2).alias("average_rating"),
        pl.sum("total_votes").alias("total_votes")
    ]).sort(["release_year", "genre"])
    year_genre_df = apply_polars_schema(year_genre_df, DATA_SCHEMA["year_genre_aggregates"])
    return year_genre_df

def transform_data(data_dict: dict[str, pl.DataFrame]) -> dict[str, pl.DataFrame]:
    movies_detailed_df = create_movies_detailed(data_dict["movies"], data_dict["ratings"])
    runtime_distribution_df = create_runtime_distribution(movies_detailed_df)
    yearly_aggregates_df, movies_year_df = create_yearly_aggregates(movies_detailed_df)
    year_genre_aggregates_df = create_year_genre_aggregates(movies_year_df)
    
    return {
        "movies_detailed": movies_detailed_df,
        "runtime_distribution": runtime_distribution_df,
        "yearly_aggregates": yearly_aggregates_df,
        "year_genre_aggregates": year_genre_aggregates_df
    }

if __name__ == "__main__":
    import logging
    from config import RAW_FILEPATHS, TRANSFORMED_FILEPATHS

    logging.info("Starting data transformation...")
    try:
        movies_detailed_df = pl.read_csv(
            RAW_FILEPATHS["MOVIES"],
            separator='\t',
            null_values='\\N',
            quote_char=None,
        )
        ratings_df = pl.read_csv(
            RAW_FILEPATHS["RATINGS"],
            separator="\t",
            null_values="\\N",
            quote_char=None,
        )
        raw_data = {
            "movies": movies_detailed_df,
            "ratings": ratings_df
        }
        transformed_data = transform_data(raw_data)
        transformed_data["movies_detailed"].write_csv(TRANSFORMED_FILEPATHS["MOVIES_DETAILED"])
        transformed_data["runtime_distribution"].write_csv(TRANSFORMED_FILEPATHS["RUNTIME_DISTRIBUTION"])
        transformed_data["yearly_aggregates"].write_csv(TRANSFORMED_FILEPATHS["YEARLY_AGGREGATES"])
        transformed_data["year_genre_aggregates"].write_csv(TRANSFORMED_FILEPATHS["YEAR_GENRE_AGGREGATES"])

        logging.info("Data transformation completed successfully.")
        for name, df in transformed_data.items():
            logging.info(f"{name} DataFrame shape: {df.shape}")
            logging.debug(f"{name} DataFrame schema: {df.schema}")
            logging.debug(f"{name} Null values:\n{df.null_count()}")
    except Exception as e:
        logging.error(f"Data transformation failed: {str(e)}")