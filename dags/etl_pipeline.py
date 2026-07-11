from airflow.sdk import dag, task
from src.utils.db_connection import get_minio_client, get_spark_session
from src.bronze.ingestion import ingest_taxi, ingest_zone
from src.silver.transform_taxi import transform_taxi
from src.silver.transform_zone import transform_zone


@dag(
    schedule=None,
    catchup=False
)
def etl_dag():

    @task
    def bronze():
        s3_client = get_minio_client()
        ingest_taxi(s3_client, list(range(2009, 2027)))
        ingest_zone(s3_client)

    @task
    def silver():
        spark = get_spark_session()

        transform_taxi(spark, list(range(2009, 2027)))
        transform_zone(spark)

        spark.stop()

    bronze() >> silver()



etl_dag()