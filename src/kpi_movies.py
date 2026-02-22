from __future__ import annotations

from pyspark.sql import DataFrame, functions as F
from typing import Iterable, Optional

from src.logging_utils import get_logger
from pathlib import Path


logger = get_logger()


def _require_cols(df: DataFrame, cols: Iterable[str], fn_name: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        msg = f"{fn_name}: missing required columns: {missing}"
        logger.error(msg)
        raise ValueError(msg)


def add_kpi_columns(df: DataFrame) -> DataFrame:
    """
    Add KPI columns used for ranking: profit_musd, roi.
    """
    _require_cols(df, ["budget_musd", "revenue_musd"], "add_kpi_columns")
    logger.info("add_kpi_columns: adding profit_musd and roi")

    return (
        df
        .withColumn("profit_musd", F.col("revenue_musd") - F.col("budget_musd"))
        .withColumn(
            "roi",
            F.when(
                (F.col("budget_musd").isNull()) | (F.col("budget_musd") == 0),
                F.lit(None)
            ).otherwise(F.col("revenue_musd") / F.col("budget_musd"))
        )
    )


def rank_movies(
    df: DataFrame,
    metric_col: str,
    n: int = 10,
    ascending: bool = False,
    filters: Optional[list] = None,
    select_cols: Optional[list[str]] = None,
) -> DataFrame:
    """
    Generic ranking helper:
      - applies optional filters
      - drops null metric
      - orders + limits
      - returns selected columns
    """
    _require_cols(df, [metric_col], "rank_movies")
    logger.info(
        "rank_movies: metric=%s n=%s ascending=%s filters=%s",
        metric_col, n, ascending, bool(filters)
    )

    out = df
    if filters:
        for cond in filters:
            out = out.filter(cond)

    out = out.filter(F.col(metric_col).isNotNull())

    order_col = F.col(metric_col).asc() if ascending else F.col(metric_col).desc()
    out = out.orderBy(order_col).limit(int(n))

    if select_cols is None:
        # including id by default 
        select_cols = ["id", "title", "release_date", metric_col]

    # validate selected cols exist
    _require_cols(out, select_cols, "rank_movies(select_cols)")
    return out.select(*select_cols)

def write_kpi_csv(df: DataFrame, out_path: str, mode: str = "overwrite", single_file: bool = True):
    """
    Write KPI dataframe to CSV.
    Spark will create a folder at out_path.
    """
    logger.info("Writing KPI CSV to %s (single_file=%s)", out_path, single_file)

    try:
        writer_df = df.coalesce(1) if single_file else df
        (
            writer_df.write
            .mode(mode)
            .option("header", "true")
            .csv(out_path)
        )
    except Exception:
        logger.exception("Failed to write KPI CSV to %s", out_path)
        raise