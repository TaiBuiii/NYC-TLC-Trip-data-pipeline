from pyspark.sql import functions as F
from src.utils.db_connection import get_spark_session
from src.utils.logger import get_logger

logger = get_logger(__name__)

def casting(df_raw):
    df = df_raw.select(
        F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_datetime"), 
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),
        F.col("trip_distance").cast("double"),
        F.col("PULocationID").cast("int").alias("pickup_location_id"),
        F.col("DOLocationID").cast("int").alias("dropoff_location_id"),
        F.col("passenger_count").cast("int"),
        F.col("payment_type").cast("int"),
        F.col("fare_amount").cast("double"),
        F.col("tip_amount").cast("double"),
        F.col("extra").cast("double"),
        F.col("total_amount").cast("double")
    )
    return df

def cleansing(df_raw):
    df = df_raw.filter(
        (F.col("trip_distance") > 0) &
        (F.col("total_amount") > 0) &
        (F.col("fare_amount") > 0) &
        (F.col("tip_amount") >= 0) &
        (F.col("dropoff_datetime") > F.col("pickup_datetime"))
    )
    return df


def feature_engineering(df_raw):
    df = df_raw \
        .withColumn("pickup_hour", F.hour(F.col("pickup_datetime"))) \
        .withColumn("pickup_day_of_week", F.dayofweek(F.col("pickup_datetime"))) \
        .withColumn("trip_distance", F.round(F.col("trip_distance") * 1.60934,2)) \
        .withColumn("trip_duration",  F.round((F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60,2)) \
        .withColumn("average_speed" ,F.round(F.col("trip_distance") / (F.col("trip_duration")/60),2))
    return df
                
def transform_taxi():
    spark = get_spark_session()
    years  = [2022]
    months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    for year in years:
        for month in months:
            bronze_path = f"s3a://bronze/{year}/{month}/"
            silver_path = f"s3a://silver/{year}/{month}/"

            df_raw = spark.read.parquet(bronze_path)
            df = casting(df_raw)
            df = cleansing(df)
            df = feature_engineering(df)

            df.write.mode("overwrite").parquet(silver_path)
            logger.info(f"Transformed {year}-{month} -> {silver_path}")
            
    spark.stop()
