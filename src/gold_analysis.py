# src/search_queries.py
from pyspark.sql import DataFrame, functions as F

def _pipe_to_array(colname: str):
    """Split a pipe-delimited string column into an array (handles NULL)."""
    return F.when(F.col(colname).isNull(), F.array()).otherwise(F.split(F.col(colname), r"\|"))

def with_array_cols(df: DataFrame) -> DataFrame:
    """
    Add helper array columns for searching.
    """
    return (
        df
        .withColumn("genres_arr", _pipe_to_array("genres"))
        .withColumn("cast_arr2", _pipe_to_array("cast"))
    )

def has_token(arr_col: str, token: str) -> F.Column:
    """Case-insensitive exact match inside an array column."""
    t = token.lower()
    return F.exists(F.col(arr_col), lambda x: F.lower(x) == F.lit(t))

def contains_text(colname: str, text: str) -> F.Column:
    """Case-insensitive substring match for string columns."""
    return F.lower(F.col(colname)).contains(text.lower())


def search1_bruce_willis_scifi_action(df: DataFrame) -> DataFrame:
    """
    Best-rated Science Fiction + Action movies starring Bruce Willis,
    sorted by vote_average desc (tie-breaker vote_count desc).
    """
    d = with_array_cols(df)

    out = (
        d
        .filter(has_token("genres_arr", "Science Fiction"))
        .filter(has_token("genres_arr", "Action"))
        .filter(contains_text("cast", "Bruce Willis"))
        .orderBy(F.desc("vote_average"), F.desc("vote_count"))
        .select("title", "release_date", "genres", "cast", "vote_average", "vote_count", "popularity")
    )
    return out


def search2_uma_thurman_tarantino(df: DataFrame) -> DataFrame:
    """
    Movies starring Uma Thurman, directed by Quentin Tarantino,
    sorted by runtime asc.
    """
    d = with_array_cols(df)

    out = (
        d
        .filter(contains_text("cast", "Uma Thurman"))
        .filter(contains_text("director", "Quentin Tarantino"))
        .orderBy(F.asc("runtime"))
        .select("title", "release_date", "runtime", "director", "cast", "vote_average", "vote_count")
    )
    return out

def add_franchise_flag(df: DataFrame) -> DataFrame:
    """
    Add is_franchise flag based on belongs_to_collection.
    """
    return df.withColumn(
        "is_franchise",
        F.when(F.col("belongs_to_collection").isNotNull(), F.lit(True)).otherwise(F.lit(False))
    )

def franchise_vs_standalone(df: DataFrame) -> DataFrame:
    """
    Compare franchise vs standalone in:
      - mean revenue
      - median ROI
      - mean budget
      - mean popularity
      - mean rating
    """
    d = add_franchise_flag(df)

    return (
        d.groupBy("is_franchise")
         .agg(
            F.count("*").alias("n_movies"),
            F.avg("revenue_musd").alias("mean_revenue_musd"),
            F.expr("percentile_approx(roi, 0.5)").alias("median_roi"),
            F.avg("budget_musd").alias("mean_budget_musd"),
            F.avg("popularity").alias("mean_popularity"),
            F.avg("vote_average").alias("mean_rating")
         )
         .orderBy(F.desc("is_franchise"))
    )

def top_franchises(df: DataFrame, min_movies: int = 2) -> DataFrame:
    """
    Most successful franchises based on:
      - number of movies
      - total & mean budget
      - total & mean revenue
      - mean rating
    Only includes franchises with at least `min_movies`.
    """
    d = df.filter(F.col("belongs_to_collection").isNotNull())

    out = (
        d.groupBy("belongs_to_collection")
         .agg(
            F.count("*").alias("n_movies"),
            F.sum("budget_musd").alias("total_budget_musd"),
            F.avg("budget_musd").alias("mean_budget_musd"),
            F.sum("revenue_musd").alias("total_revenue_musd"),
            F.avg("revenue_musd").alias("mean_revenue_musd"),
            F.avg("vote_average").alias("mean_rating")
         )
         .filter(F.col("n_movies") >= min_movies)
         .orderBy(F.desc("total_revenue_musd"))
    )
    return out

def top_directors(df: DataFrame, min_movies: int = 2) -> DataFrame:
    """
    Most successful directors based on:
      - number of movies directed
      - total revenue
      - mean rating
    Only includes directors with at least `min_movies`.
    """
    d = df.filter(F.col("director").isNotNull())

    out = (
        d.groupBy("director")
         .agg(
            F.count("*").alias("n_movies"),
            F.sum("revenue_musd").alias("total_revenue_musd"),
            F.avg("roi").alias("mean_roi"),
            F.avg("vote_average").alias("mean_rating")
         )
         .filter(F.col("n_movies") >= min_movies)
         .orderBy(F.desc("total_revenue_musd"))
    )
    return out