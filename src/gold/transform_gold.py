from src.gold.dim_location import transform_dim_location
from src.gold.dim_payment import transform_dim_payment
from src.gold.fact_trip import transform_fact_trip
from src.utils.logger import get_logger

logger = get_logger(__name__)


def process_month(spark, engine, year, month):
    """
    Processes individual monthly data to populate Gold layer tables.

    Reads the pre-processed Silver Parquet data for a specific month, then 
    orchestrates the transformation and loading of dim_payment and fact_trip tables 
    into the target database.

    Args:
        spark (pyspark.sql.SparkSession): The active Spark session.
        engine (sqlalchemy.engine.Engine): The database engine used to write data.
        year (int): The target year to process.
        month (int): The target month to process.

    Returns:
        None
    """
    df_taxi = spark.read.parquet(f"s3a://silver/{year}/{month:02d}")

    logger.info(f"Processing dim_payment: {year}/{month:02d}")
    transform_dim_payment(engine, df_taxi)

    logger.info(f"Processing fact_trip: {year}/{month:02d}")
    transform_fact_trip(df_taxi)


def transform_gold(spark, engine, years):
    """
    Orchestrates the overall construction of the Gold layer (Star Schema).

    This function initializes the pipeline by processing dimension tables 
    (like dim_location), then iterates through the provided years and months to 
    populate the fact_trip tables and remaining dim_payment.

    Args:
        spark (pyspark.sql.SparkSession): The active Spark session.
        engine (sqlalchemy.engine.Engine): The database engine used for SQL operations.
        years (list[int]): A list of years to be processed in the pipeline.

    Returns:
        None
    """
    logger.info("Processing dim_location")
    
    df_zone = spark.read.parquet(f"s3a://silver/misc")
    transform_dim_location(engine, df_zone)

    for year in years:
        for month in range(1, 13):
            try:
                process_month(spark, engine, year, month)
            except Exception as e:
                logger.error(f"Error occured: {e}")
                continue

    logger.info("Gold layer processing completed")

