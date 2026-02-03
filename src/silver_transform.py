import os
from pyspark.sql import SparkSession, functions as F

# ======================
# Helper Functions
# ======================
def empty_to_null(col):
    return F.when(F.trim(col) == "", F.lit(None)).otherwise(col)

def pipe_from_array(col_expr):
    """
    Convert an array of structs with field 'name' into a |-delimited string.
    """
    return empty_to_null(
        F.concat_ws("|", F.expr(f"transform({col_expr}, x -> x.name)"))
    )
def zero_to_null(col):
    return F.when(col.isNull() | (col == 0), F.lit(None)).otherwise(col)

def normalize_text(col):
    """
    Convert empty strings and known placeholders to NULL.
    """
    c = empty_to_null(F.col(col))
    return F.when(
        F.lower(F.trim(c)).isin("no data", "n/a", "na", "none", "null", "tbd", "unknown"),
        F.lit(None)
    ).otherwise(c)

def add_cleaning_rules(df, min_non_null_cols=10):
    # 1) Text placeholders -> NULL
    df = (df
        .withColumn("overview", normalize_text("overview"))
        .withColumn("tagline", normalize_text("tagline"))
    )

    # 2) 0 -> NULL for numeric fields
    df = (df
        .withColumn("budget", zero_to_null(F.col("budget")))
        .withColumn("revenue", zero_to_null(F.col("revenue")))
        .withColumn("runtime", zero_to_null(F.col("runtime")))
    )

    # 3) vote_count == 0 -> vote_average NULL
    df = df.withColumn("vote_average",
        F.when(F.col("vote_count") == 0, F.lit(None)).otherwise(F.col("vote_average"))
    )

    # 4) Convert to million USD
    df = (df
        .withColumn("budget_musd", F.when(F.col("budget").isNull(), None).otherwise(F.col("budget") / 1_000_000))
        .withColumn("revenue_musd", F.when(F.col("revenue").isNull(), None).otherwise(F.col("revenue") / 1_000_000))
    )

    # 5) Drop duplicates; drop rows with unknown id/title
    df = (df
        .dropDuplicates(["id"])
        .filter(F.col("id").isNotNull() & F.col("title").isNotNull() & (F.trim(F.col("title")) != ""))
    )

    # 6) Keep only rows where at least N columns are non-null
    cols_to_check = [c for c in df.columns]  # includes derived columns too
    non_null_count = sum(F.when(F.col(c).isNotNull(), F.lit(1)).otherwise(F.lit(0)) for c in cols_to_check)
    df = df.withColumn("_non_null_cols", non_null_count).filter(F.col("_non_null_cols") >= min_non_null_cols).drop("_non_null_cols")

    # 7) Only Released movies, then drop status
    df = df.filter(F.col("status") == F.lit("Released"))
    df = df.drop("status")
    return df

# ======================
# Feature builders
# ======================
def add_genres(df):
    return df.withColumn(
        "genres",
        pipe_from_array("movie.genres")
    )

def add_collection(df):
    return df.withColumn(
        "belongs_to_collection",
        empty_to_null(F.col("movie.belongs_to_collection.name"))
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
        pipe_from_array("movie.spoken_languages")
    )
def add_production_countries(df):
    return df.withColumn(
        "production_countries",
        pipe_from_array("movie.production_countries")
    )
def add_production_companies(df):
    return df.withColumn(
        "production_companies",
        pipe_from_array("movie.production_companies")
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
def b(df, keep_arrays = True):
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

    # Genres
    out = add_genres(out)

    # Belongs to Collection
    out = add_collection(out)
    # Credits
    out = add_credits_features(out)
    # Languages / Countries / Companies
    out = add_spoken_languages(out)
    out = add_production_countries(out)
    out = add_production_companies(out)
    # Cleaning 
    out = add_cleaning_rules(out, min_non_null_cols=10)
    #  Dropping irrelevant columns (Helpers) for Final clean up
    out = out.drop("movie")
    if not keep_arrays:
        out = out.drop("cast_arr", "crew_arr")
    return out
