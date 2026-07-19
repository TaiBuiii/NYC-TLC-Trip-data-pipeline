from pyspark.sql import functions as F
from sqlalchemy import text

from src.utils.logger import get_logger

logger = get_logger(__name__)


def create_dim_payment(df):
    """
    transform the data accordingly to the dim_payment table's structure. Specfically, it creates a new dataframe containing distinct payment_id
    and mapping with the specified value
    Args:
        - df: pyspark.sql.DataFrame taxi data in the silver layer
    Returns:
        - df: pyspark.sql.DataFrame dim_payment dataframe
    """
    payment_df = (
        df.select(F.col("payment_id")).distinct()
        .filter(F.col("payment_id").isNotNull())
    )

    mapping = F.create_map(
        F.lit(0), F.lit("Unknown"),
        F.lit(1), F.lit("Credit card"),
        F.lit(2), F.lit("Cash"),
        F.lit(3), F.lit("No charge"),
        F.lit(4), F.lit("Dispute")
    )

    payment_df = payment_df.withColumn("payment_name", mapping[F.col("payment_id")]).orderBy("payment_id")

    return payment_df


def upsert_dim_payment(engine, df):
    """
    Performs upsert into the dim_payment. Notably, only insert new records that do are not existing in the dim_payment 
    Args:
        - engine (sqlalchemy.engine.Engine): The database engine used for SQL operations.
        - df: pyspark.sql.DataFrame dim_payment dataframe
    Returns:
        None
    """

    sql = """
    INSERT INTO star_schema.dim_payment(
        payment_id,
        payment_name
    )
    VALUES(
        :payment_id,
        :payment_name
    )
    ON CONFLICT(payment_id) DO NOTHING;
    """

    rows = [row.asDict() for row in df.collect()]

    with engine.begin() as conn:
        conn.execute(text(sql), rows)


def transform_dim_payment(engine, df):
    """
    Orchestrates the transformation of the payment's dimension. Specifically, it transforms the taxi data according to the
    payment's dimension table and upserts the data into Postgres.
    Args:
        - engine (sqlalchemy.engine.Engine): The database engine used for SQL operations.
        - df: pyspark.sql.DataFrame taxi data in the silver layer
    Returns:
        None
    """
    logger.info("Building payment dimension")
    dim_payment = create_dim_payment(df)

    upsert_dim_payment(engine, dim_payment)
    logger.info("Payment dimension completed")