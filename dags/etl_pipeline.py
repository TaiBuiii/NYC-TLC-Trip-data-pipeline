from airflow.sdk import dag, task
from sqlalchemy import text
from src.utils.db_connection import get_minio_client, get_spark_session, get_postgres_engine
from src.bronze.ingestion import ingest_taxi, ingest_zone
from src.silver.transform_taxi import transform_taxi
from src.silver.transform_zone import transform_zone
from src.gold.transform_gold import transform_gold
from datetime import timedelta
@dag(
    schedule=None,
    catchup=False
)
def etl_dag():
    """
    Orchestrates the end-to-end data pipeline:
    1. Bronze: Ingests raw data from source to MinIO.
    2. Silver: Performs cleaning and transformations using PySpark.
    3. Gold: Loads transformed data into PostgreSQL star schema.
    """
    @task(retries=3, retry_delay=timedelta(minutes=5))
    def bronze():
        """
        Extracts raw taxi and zone data and loads it into the Bronze layer (MinIO).
        """
        s3_client = get_minio_client()
        ingest_taxi(s3_client, list(range(2025, 2027)))
        ingest_zone(s3_client)

    @task(retries=3, retry_delay=timedelta(minutes=5))
    def silver():
        """
        Cleans and processes data from the Bronze layer, storing it in the Silver layer (MinIO).
        """
        spark = get_spark_session()

        transform_taxi(spark, list(range(2025, 2027)))
        transform_zone(spark)

        spark.stop()

    @task(retries=3, retry_delay=timedelta(minutes=5))
    def gold():
        """
        Applies final business logic and populates the PostgreSQL Star Schema.
        """
        spark = get_spark_session()
        engine = get_postgres_engine()

        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE star_schema.fact_trip RESTART IDENTITY"))

        transform_gold(spark, engine, list(range(2025, 2027)))
        spark.stop()

    bronze() >> silver() >> gold()



etl_dag()