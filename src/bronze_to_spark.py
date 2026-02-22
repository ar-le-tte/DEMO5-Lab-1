from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
from src.logging_utils import get_logger

logger = get_logger()


def read_bronze_json(
    spark: SparkSession,
    bronze_dir: str,
    multiline: bool = True
) -> DataFrame:
    """
    Read TMDB Bronze JSON files in a directory into a Spark DataFrame,
    with basic validation (fails if no records are found).
    """
    logger.info("read_bronze_json: reading bronze JSON from %s (multiLine=%s)", bronze_dir, multiline)

    try:
        df = (
            spark.read
                 .option("multiLine", str(multiline).lower())
                 .json(bronze_dir)
        )

        logger.info("read_bronze_json: schema columns=%s", df.columns)

        # Validation 
        if df.rdd.isEmpty():
            msg = f"read_bronze_json: No JSON records found in path: {bronze_dir}"
            logger.error(msg)
            raise ValueError(msg)

        logger.info("read_bronze_json: successfully loaded bronze data")
        return df

    except Exception:
        logger.exception("read_bronze_json failed for path=%s", bronze_dir)
        raise