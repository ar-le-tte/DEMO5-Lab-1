from pyspark.sql import SparkSession, DataFrame


def read_bronze_json(spark: SparkSession, bronze_dir: str,
    multiline: bool = True) -> DataFrame:
    """
    Read TMDB Bronze JSON files in a directory into a Spark DataFrame.
    """
    df = (
        spark.read
             .option("multiLine", multiline)
             .json(bronze_dir)
    )
    return df
