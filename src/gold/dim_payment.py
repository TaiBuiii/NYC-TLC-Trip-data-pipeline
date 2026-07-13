from pyspark.sql import functions as F
from sqlalchemy import text

from src.utils.logger import get_logger

logger = get_logger(__name__)


def create_dim_payment(df):
    payment_df = (
        df.select(F.col("payment_id"))
        .distinct()
        .filter(F.col("payment_id").isNotNull())
    )

    mapping = F.create_map(
        F.lit(1), F.lit("Credit card"),
        F.lit(2), F.lit("Cash"),
        F.lit(3), F.lit("No charge"),
        F.lit(4), F.lit("Dispute"),
        F.lit(5), F.lit("Unknown"),
        F.lit(6), F.lit("Voided trip")
    )

    payment_df = payment_df.withColumn("payment_name", mapping[F.col("payment_id")]).orderBy("payment_id")

    return payment_df


def upsert_dim_payment(engine, df):

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

    if not rows:
        return

    with engine.begin() as conn:
        conn.execute(text(sql), rows)

def transform_dim_payment(engine, df):
    logger.info("Building payment dimension")
    dim_payment = create_dim_payment(df)
    upsert_dim_payment(engine, dim_payment)
    logger.info("Payment dimension completed")