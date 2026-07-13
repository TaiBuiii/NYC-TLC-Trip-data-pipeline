from pyspark.sql import functions as F
from src.gold.dim_location import transform_dim_location
from src.gold.dim_payment import transform_dim_payment
from src.gold.fact_trip import transform_fact_trip
from src.utils.logger import get_logger

logger = get_logger(__name__)

def load_taxi(spark, year, month):
    silver = f"s3a://silver/{year}/{month:02d}/"
    logger.info(f"Loading s3a://silver/{year}/{month:02d}/")
    return spark.read.parquet(silver)

def load_zone(spark):
    silver = f"s3a://silver/misc/"
    logger.info(f"Loading s3a://silver/misc/")
    return spark.read.parquet(silver)

def transform_gold(spark, engine, years):
    logger.info("Starting gold layer processing")

    df_zone = load_zone(spark)
    logger.info("Loaded zone data for dimensions")
    transform_dim_location(engine, df_zone)

    valid_location_ids = [row.location_id for row in df_zone.select("location_id").collect()]

    payment_loaded = False

    for year in years:
        for month in range(1, 13):
            try:
                df_taxi = load_taxi(spark, year, month)
                logger.info(f"Processing gold data for {year}-{month:02d}")

                if not payment_loaded:
                    transform_dim_payment(engine, df_taxi)
                    payment_loaded = True

                df_taxi = df_taxi.filter(
                    F.col("pickup_location_id").isin(valid_location_ids) &
                    F.col("dropoff_location_id").isin(valid_location_ids) &
                    F.col("payment_id").isin(1, 2, 3, 4, 5, 6)
                )

                transform_fact_trip(df_taxi)
            except Exception as e:
                logger.error(f"Error occured: {e}")
                continue

    logger.info("Gold layer processing completed")

