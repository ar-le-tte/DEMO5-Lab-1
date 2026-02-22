from __future__ import annotations

from typing import Iterable
from pyspark.sql import DataFrame, functions as F

from src.logging_utils import get_logger
from src.kpi_movies import _require_cols
logger = get_logger()

def _pipe_to_array(colname: str) -> F.Column:
    """Split a pipe-delimited string column into an array (handles NULL/empty)."""
    c = F.col(colname)
    return F.when(c.isNull() | (F.trim(c) == ""), F.array().cast("array<string>")) \
            .otherwise(F.split(c, r"\|"))


def with_array_cols(df: DataFrame) -> DataFrame:
    """Add helper array columns for searching."""
    _require_cols(df, ["genres", "cast"], "with_array_cols")
    logger.info("with_array_cols: adding genres_arr and cast_arr")
    return (
        df
        .withColumn("genres_arr", _pipe_to_array("genres"))
        .withColumn("cast_arr", _pipe_to_array("cast"))
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
    try:
        _require_cols(df, ["genres", "cast", "vote_average", "vote_count", "popularity", "release_date", "title"], "search1_bruce_willis_scifi_action")
        d = with_array_cols(df)

        out = (
            d.filter(has_token("genres_arr", "Science Fiction"))
             .filter(has_token("genres_arr", "Action"))
             .filter(has_token("cast_arr", "Bruce Willis"))
             .orderBy(F.desc("vote_average"), F.desc("vote_count"))
             .select("title", "release_date", "genres", "cast", "vote_average", "vote_count", "popularity")
        )
        logger.info("search1_bruce_willis_scifi_action: query built")
        return out

    except Exception:
        logger.exception("search1_bruce_willis_scifi_action failed")
        raise


def search2_uma_thurman_tarantino(df: DataFrame) -> DataFrame:
    """
    Movies starring Uma Thurman, directed by Quentin Tarantino,
    sorted by runtime asc.
    """
    try:
        _require_cols(df, ["cast", "director", "runtime", "title", "release_date", "vote_average", "vote_count"], "search2_uma_thurman_tarantino")
        d = with_array_cols(df)

        out = (
            d.filter(has_token("cast_arr", "Uma Thurman"))
             .filter(contains_text("director", "Quentin Tarantino"))
             .orderBy(F.asc("runtime"))
             .select("title", "release_date", "runtime", "director", "cast", "vote_average", "vote_count")
        )
        logger.info("search2_uma_thurman_tarantino: query built")
        return out

    except Exception:
        logger.exception("search2_uma_thurman_tarantino failed")
        raise


def add_franchise_flag(df: DataFrame) -> DataFrame:
    """Add is_franchise flag based on belongs_to_collection."""
    _require_cols(df, ["belongs_to_collection"], "add_franchise_flag")
    logger.info("add_franchise_flag: adding is_franchise boolean")
    return df.withColumn("is_franchise", F.col("belongs_to_collection").isNotNull())


def franchise_vs_standalone(df: DataFrame) -> DataFrame:
    """
    Compare franchise vs standalone:
      mean revenue, median ROI, mean budget, mean popularity, mean rating.
    """
    try:
        _require_cols(df, ["belongs_to_collection", "revenue_musd", "roi", "budget_musd", "popularity", "vote_average"], "franchise_vs_standalone")
        d = add_franchise_flag(df)

        out = (
            d.groupBy("is_franchise")
             .agg(
                F.count("*").alias("n_movies"),
                F.avg("revenue_musd").alias("mean_revenue_musd"),
                F.expr("percentile_approx(roi, 0.5)").alias("median_roi"),
                F.avg("budget_musd").alias("mean_budget_musd"),
                F.avg("popularity").alias("mean_popularity"),
                F.avg("vote_average").alias("mean_rating"),
             )
             .orderBy(F.desc("is_franchise"))
        )
        logger.info("franchise_vs_standalone: aggregation built")
        return out

    except Exception:
        logger.exception("franchise_vs_standalone failed")
        raise


def top_franchises(df: DataFrame, min_movies: int = 2) -> DataFrame:
    """Most successful franchises (min_movies filter)."""
    try:
        _require_cols(df, ["belongs_to_collection", "budget_musd", "revenue_musd", "vote_average"], "top_franchises")
        d = df.filter(F.col("belongs_to_collection").isNotNull())

        out = (
            d.groupBy("belongs_to_collection")
             .agg(
                F.count("*").alias("n_movies"),
                F.sum("budget_musd").alias("total_budget_musd"),
                F.avg("budget_musd").alias("mean_budget_musd"),
                F.sum("revenue_musd").alias("total_revenue_musd"),
                F.avg("revenue_musd").alias("mean_revenue_musd"),
                F.avg("vote_average").alias("mean_rating"),
             )
             .filter(F.col("n_movies") >= int(min_movies))
             .orderBy(F.desc("total_revenue_musd"))
        )
        logger.info("top_franchises: aggregation built (min_movies=%s)", min_movies)
        return out

    except Exception:
        logger.exception("top_franchises failed (min_movies=%s)", min_movies)
        raise


def top_directors(df: DataFrame, min_movies: int = 2) -> DataFrame:
    """Most successful directors (min_movies filter)."""
    try:
        _require_cols(df, ["director", "revenue_musd", "roi", "vote_average"], "top_directors")
        d = df.filter(F.col("director").isNotNull())

        out = (
            d.groupBy("director")
             .agg(
                F.count("*").alias("n_movies"),
                F.sum("revenue_musd").alias("total_revenue_musd"),
                F.avg("roi").alias("mean_roi"),
                F.avg("vote_average").alias("mean_rating"),
             )
             .filter(F.col("n_movies") >= int(min_movies))
             .orderBy(F.desc("total_revenue_musd"))
        )
        logger.info("top_directors: aggregation built (min_movies=%s)", min_movies)
        return out

    except Exception:
        logger.exception("top_directors failed (min_movies=%s)", min_movies)
        raise