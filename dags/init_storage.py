from airflow.sdk import dag, task
from botocore.exceptions import ClientError
from datetime import timedelta
from src.utils.db_connection import get_minio_client
from src.utils.logger import get_logger

logger = get_logger(__name__)

BUCKETS = ["bronze", "silver"]


def bucket_exists(client, bucket):
    """
    Checks if a specific S3 bucket exists by sending a HEAD request.
    """
    try:
        client.head_bucket(Bucket=bucket)
        return True
    except ClientError:
        return False


def create_bucket_if_not_exists(client, bucket):
    """
    Creates an S3 bucket if it does not already exist.
    """
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
    """
    Airflow DAG to initialize storage architecture by creating required S3 buckets.
    """
    @task(retries=3, retry_delay=timedelta(minutes=5))
    def create_buckets():
        """
        Connects to MinIO and iterates through the bucket list to ensure they exist.
        """
        client = get_minio_client()

        for bucket in BUCKETS:
            create_bucket_if_not_exists(client, bucket)

    create_buckets()


init_storage()