from pyspark.sql import functions as F
from sqlalchemy import text

from src.utils.logger import get_logger

logger = get_logger(__name__)


def create_dim_location(df):
    """
    This function simply selects distinct values in zone data and order the data by location_id
    Args:
        - df: pyspark.sql.DataFrame zone data in the silver layer
    Returns:
        - df: pyspark.sql.DataFrame dim_location data
    """
    return (
        df.select(
            "location_id",
            "borough",
            "zone",
            "service_zone"
        )
        .dropDuplicates(["location_id"])
        .orderBy("location_id")
    )

def upsert_dim_location(engine, df):
    """
    Performs upsert into the dim_location. Notably, only insert new records that do are not existing in the dim_location 
    Args:
        - engine (sqlalchemy.engine.Engine): The database engine used for SQL operations.
        - df: pyspark.sql.DataFrame dim_location dataframe
    Returns:
        None
    """
    sql = """
    INSERT INTO star_schema.dim_location(
        location_id,
        borough,
        zone,
        service_zone
    )
    VALUES(
        :location_id,
        :borough,
        :zone,
        :service_zone
    )
    ON CONFLICT(location_id)
    DO NOTHING;
    """
    rows = [row.asDict() for row in df.collect()]

    with engine.begin() as conn:
        conn.execute(text(sql), rows)

def transform_dim_location(engine, df):
    """
    Orchestrates the transformation of the location's dimension. Specifically, it transforms the zone data according to the
    location's dimension table and upserts the data into Postgres.
    Args:
        - engine (sqlalchemy.engine.Engine): The database engine used for SQL operations.
        - df: pyspark.sql.DataFrame zone data in the silver layer
    Returns:
        None
    """   
    logger.info("Building location dimension")
    dim_location = create_dim_location(df)

    upsert_dim_location(engine, dim_location)
    logger.info("Location dimension completed")