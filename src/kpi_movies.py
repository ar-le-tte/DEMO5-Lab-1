# src/kpi_movies.py
from pyspark.sql import DataFrame, functions as F, Window

def add_kpi_columns(df: DataFrame) -> DataFrame:
    """
    KPI columns used for ranking.
    """
    out = (
        df
        .withColumn("profit_musd", F.col("revenue_musd") - F.col("budget_musd"))
        .withColumn(
            "roi",
            F.when(
                (F.col("budget_musd").isNull()) | (F.col("budget_musd") == 0),
                None
            ).otherwise(F.col("revenue_musd") / F.col("budget_musd"))
        )
    )
    return out


def rank_movies(df: DataFrame, metric_col: str, n: int = 10, ascending: bool = False,
                filters: list = None, select_cols: list[str] | None = None) -> DataFrame:
    """
    Generic ranking helper.
    """
    out = df
    if filters:
        for cond in filters:
            out = out.filter(cond)

    out = out.filter(F.col(metric_col).isNotNull())

    order_col = F.col(metric_col).asc() if ascending else F.col(metric_col).desc()
    out = out.orderBy(order_col).limit(n)

    # default display cols
    if select_cols is None:
        select_cols = ["title", "release_date", metric_col]

    return out.select(*select_cols)
