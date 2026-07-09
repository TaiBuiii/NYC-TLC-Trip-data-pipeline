import os
import boto3
from dotenv import load_dotenv  
import sqlalchemy

load_dotenv() 


def get_minio_client():
    """
    Initialize and return S3 Client, working with MinIO
    """
    try:
        s3_client = boto3.client( 
            's3',
            aws_access_key_id = os.getenv("MINIO_ROOT_USER", "minioadmin"),
            aws_secret_access_key = os.getenv("MINIO_ROOT_PASSWORD","minioadmin"),
            endpoint_url = f"http://minio:{os.getenv('MINIO_API_PORT')}/"
        )
        return s3_client 
    except Exception as e:
        raise ConnectionError(f"Cannot connecto MinIO: {e}") from e


def get_postgres_conn():
    """
    Initialize Postgres connection 
    """
    try:
        conn_str = f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@postgres:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
        engine = sqlalchemy.create_engine(conn_str)
        return engine
    except Exception as e:
        raise ConnectionError(f"Cannot connect to Postgres: {e}") from e
