from pyspark.sql import functions as F
from sqlalchemy import text

from src.utils.logger import get_logger

logger = get_logger(__name__)


def create_dim_location(df):
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
    logger.info("Building location dimension")
    dim_location = create_dim_location(df)
    upsert_dim_location(engine, dim_location)
    logger.info("Location dimension completed")