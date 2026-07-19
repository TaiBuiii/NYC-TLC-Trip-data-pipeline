from pyspark.sql import functions as F
from src.utils.logger import get_logger

logger = get_logger(__name__)

def casting(df_raw):
    """
    Casting to appropriate data type and trim string of New York's zone data
    Args:
        - df_raw: pyspark.sql.DataFrame before casting
    Returns:
        - df_raw: pyspark.sql.DataFrame after casting
    """
    df = df_raw.select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.trim(F.col("Borough").cast("string")).alias("borough"),
        F.trim(F.col("Zone").cast("string")).alias("zone"),
        F.trim(F.col("service_zone").cast("string")).alias("service_zone")
    )
    return df

def transform_zone(spark):
    """
    Orchestrates the New York's zone data transformation process.
    Args:
        - spark (pyspark.sql.SparkSession): The active Spark session.
    Returns:
        None: The function writes the transformed DataFrames directly to storage.
    """
    try:
        df_raw = spark.read.csv("s3a://bronze/misc/taxi_zone_lookup.csv", header=True)
        df = casting(df_raw)
        df.write.mode("overwrite").parquet("s3a://silver/misc/")

        logger.info("Transform Zone successfully")
    except:
        logger.error("Failed transforming Zone")