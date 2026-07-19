from src.utils.db_connection import write_postgres_table
from src.utils.logger import get_logger
from pyspark.sql import functions as F

logger = get_logger(__name__)


def transform_fact_trip(df):
    """
    This function selects essential fields of taxi data, and write to fact_trip via 
    write_postgres_table() function.
    Args:
        - df: pyspark dataframe in the silver layer
    Returns:
        - None
    """
    try:
        write_postgres_table(
            df.select(
                F.col("pickup_datetime"),
                F.col("dropoff_datetime"),
                F.col("pickup_location_id"),
                F.col("dropoff_location_id"),
                F.col("payment_id"),
                F.col("trip_distance"),
                F.col("passenger_count"),
                F.col("fare_amount"),
                F.col("tip_amount"),
                F.col("extra"),
                F.col("total_amount"),
                F.col("trip_duration"),
                F.col("average_speed"),
            ), "star_schema.fact_trip")
        
        logger.info("fact_trip loaded successfully")
    except Exception as e:
        logger.error(f"Failed to load fact_trip: {e}")
        raise