from pyspark.sql import functions as F
from src.utils.db_connection import get_spark_session
from src.utils.logger import get_logger

logger = get_logger(__name__)

def casting(df_raw):
    df = df_raw.select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.trim(F.col("Borough").cast("string")).alias("borough"),
        F.trim(F.col("Zone").cast("string")).alias("zone"),
        F.trim(F.col("service_zone").cast("string")).alias("service_zone")
    )
    return df

def transform_zone():
    spark = get_spark_session()
    df_raw = spark.read.csv("s3a://bronze/misc/taxi_zone_lookup.csv", header=True)
    df = casting(df_raw)
    df.write.mode("overwrite").parquet("s3a://silver/misc/")
    logger.info("Transformed taxi_zone -> silver")
    spark.stop()
