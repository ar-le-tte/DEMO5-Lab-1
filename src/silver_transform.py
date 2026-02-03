import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql import DataFrame

# ======================
# Helper Functions
# ======================
def empty_to_null(col):
    return F.when(F.trim(col) == "", F.lit(None)).otherwise(col)

def pipe_from_array(col_expr: str, field: str = "name"):
    """
    Convert an array of structs with field 'name' into a |-delimited string.
    """
    return empty_to_null(
        F.concat_ws("|", F.expr(f"transform({col_expr}, x -> x.{field})"))
    )
def zero_to_null_cols(df: DataFrame, cols: list[str]) -> DataFrame:
    """
    Replace 0 with NULL for selected numeric columns.
    """
    for c in cols:
        df = df.withColumn(c, F.when(F.col(c) == 0, None).otherwise(F.col(c)))
    return df

def value_counts(df, col, top_n=20, drop_nulls=False):
    """
    Spark equivalent of pandas value_counts().
    """
    out = df
    if drop_nulls:
        out = out.filter(F.col(col).isNotNull())

    return (
        out
        .groupBy(col)
        .count()
        .orderBy(F.desc("count"))
        .limit(top_n)
    )
def nullify_text_placeholders(df: DataFrame, cols: list[str], placeholders: list[str] | None = None) -> DataFrame:
    """
    Convert empty strings and known placeholder text to NULL for the specified columns.
    """
    if placeholders is None:
        placeholders = ["no data", "n/a", "na", "none", "null", "tbd", "unknown", ""]

    for c in cols:
        df = df.withColumn(
            c,
            F.when(
                F.lower(F.trim(F.col(c))).isin([p.lower() for p in placeholders]),
                None
            ).otherwise(F.col(c)))
    return df
def add_musd_columns(df: DataFrame) -> DataFrame:
    """
    Add budget_musd and revenue_musd columns (million USD).
    """
    return (
        df
        .withColumn("budget_musd", F.when(F.col("budget").isNull(), None).otherwise(F.col("budget") / 1_000_000))
        .withColumn("revenue_musd", F.when(F.col("revenue").isNull(), None).otherwise(F.col("revenue") / 1_000_000))
    )

def nullify_vote_average_when_no_votes(df: DataFrame) -> DataFrame:
    """
    If vote_count == 0, vote_average is not meaningful -> set to NULL.
    """
    return df.withColumn(
        "vote_average",
        F.when(F.col("vote_count") == 0, None).otherwise(F.col("vote_average"))
    )

# ======================
# Feature builders
# ======================
def add_genres(df):
    return df.withColumn(
        "genres",
        pipe_from_array("genres_raw")
    )

def add_collection(df):
    return df.withColumn(
        "belongs_to_collection",
        empty_to_null(F.col("belongs_to_collection_raw.name"))
    )

def add_credits_features(df, cast_top_n=30):
    # director: first crew member where job == 'Director'
    director = F.expr("filter(crew_arr, x -> x.job = 'Director')[0].name")

    # cast names: take top N by 'order' (already roughly sorted), then join with |
    cast_names = F.expr(f"transform(slice(cast_arr, 1, {cast_top_n}), x -> x.name)")

    return (df
        .withColumn("director", director)
        .withColumn("cast_size", F.size("cast_arr"))
        .withColumn("crew_size", F.size("crew_arr"))
        .withColumn("cast", F.concat_ws("|", cast_names))
        .withColumn("director", empty_to_null(F.col("director")))
        .withColumn("cast", empty_to_null(F.col("cast")))
    )
def add_spoken_languages(df):
    return df.withColumn(
        "spoken_languages",
        pipe_from_array("spoken_languages_raw", field="english_name")
    )
def add_production_countries(df):
    return df.withColumn(
        "production_countries",
        pipe_from_array("production_countries_raw")
    )
def add_production_companies(df):
    return df.withColumn(
        "production_companies",
        pipe_from_array("production_companies_raw")
    )
def cast_movie_types(df: DataFrame) -> DataFrame:
    """
    Enforce consistent datatypes for analysis-ready Silver.
    """
    def safe_cast(col, dtype):
        return F.when(F.trim(F.col(col)) == "", None).otherwise(F.col(col).cast(dtype))
    return (
        df
        .withColumn("id", F.col("id").cast("long"))
        .withColumn("budget", F.col("budget").cast("double"))
        .withColumn("revenue", F.col("revenue").cast("double"))
        .withColumn("popularity", F.col("popularity").cast("double"))
        .withColumn("runtime", F.col("runtime").cast("double"))
        .withColumn("vote_count", F.col("vote_count").cast("long"))
        .withColumn("vote_average", F.col("vote_average").cast("double"))
        .withColumn("release_date", F.to_date("release_date"))
    )
# ======================
# Columns to keep
# ======================

def build_silver(df):
    """
    Build the Silver movies table from TMDB Bronze JSON. Keeping only the Relevant columns

    """
    out = df.select(
        F.col("movie.id").alias("id"),
        F.col("movie.title").alias("title"),
        F.to_date("movie.release_date").alias("release_date"),
        F.col("movie.original_language").alias("original_language"),
        F.col("movie.belongs_to_collection").alias("belongs_to_collection_raw"),
        F.col("movie.genres").alias("genres_raw"),
        F.col("movie.spoken_languages").alias("spoken_languages_raw"),
        F.col("movie.production_countries").alias("production_countries_raw"),
        F.col("movie.production_companies").alias("production_companies_raw"),
        F.col("movie.budget").cast("double").alias("budget"),
        F.col("movie.revenue").cast("double").alias("revenue"),
        F.col("movie.runtime").cast("double").alias("runtime"),
        F.col("movie.vote_count").cast("long").alias("vote_count"),
        F.col("movie.vote_average").cast("double").alias("vote_average"),
        F.col("movie.popularity").cast("double").alias("popularity"),
        F.col("movie.overview").alias("overview"),
        F.col("movie.tagline").alias("tagline"),
        F.col("movie.poster_path").alias("poster_path"),
        F.col("movie.status").alias("status"),
    )
    return out

