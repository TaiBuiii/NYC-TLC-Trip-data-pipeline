from airflow.sdk import dag, task
from botocore.exceptions import ClientError

from src.utils.db_connection import get_minio_client
from src.utils.logger import get_logger

logger = get_logger(__name__)

BUCKETS = ["bronze", "silver"]


def bucket_exists(client, bucket):
    try:
        client.head_bucket(Bucket=bucket)
        return True
    except ClientError:
        return False


def create_bucket_if_not_exists(client, bucket):
    if bucket_exists(client, bucket):
        logger.info(f"Bucket '{bucket}' already exists")
        return

    client.create_bucket(Bucket=bucket)
    logger.info(f"Created bucket '{bucket}'")


@dag(
    schedule=None,
    catchup=False,
)
def init_storage():

    @task
    def create_buckets():
        client = get_minio_client()

        for bucket in BUCKETS:
            create_bucket_if_not_exists(client, bucket)

    create_buckets()


init_storage()