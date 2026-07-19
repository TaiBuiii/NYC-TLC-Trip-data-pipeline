import os
import boto3
from dotenv import load_dotenv  
import sqlalchemy
from pyspark.sql import SparkSession
load_dotenv() 
from src.utils.logger import get_logger

logger = get_logger(__name__)

def get_jdbc_url():
    """
    Constructs the JDBC connection URL for PostgreSQL.
    Returns:
        str: The full JDBC URL string.
    """
    return (
        f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}"
        f":{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB')}"
    )

def write_postgres_table(df, table_name, mode="append"):
    """
    Writes a PySpark DataFrame to a PostgreSQL table using JDBC.
    Args:
        - df (pyspark.sql.DataFrame): The DataFrame to write.
        - table_name (str): Destination table name.
        - mode (str): Write mode ('append', 'overwrite', etc.). Defaults to 'append'.
    """
    df.write.format("jdbc") \
        .option("url", get_jdbc_url()) \
        .option("dbtable", table_name) \
        .option("user", os.getenv("POSTGRES_USER")) \
        .option("password", os.getenv("POSTGRES_PASSWORD")) \
        .option("driver", "org.postgresql.Driver") \
        .mode(mode) \
        .save()


def get_minio_client():
    """
    Initializes and returns a boto3 S3 client for MinIO interaction.
    Returns:
        boto3.client: Configured S3 client.
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


def get_postgres_engine():
    """
    Initialize Postgres connection 
    Returns:
        sqlalchemy.engine.Engine: Database engine instance.
    """
    try:
        conn_str = f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@postgres:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
        return sqlalchemy.create_engine(conn_str)
    
    except Exception as e:
        raise ConnectionError(f"Cannot connect to Postgres: {e}") from e


def get_spark_session():
    """
    Initializes a SparkSession with necessary configurations for S3/MinIO and PostgreSQL access.
    Includes JAR dependencies for Hadoop-AWS and PostgreSQL driver.
    Returns:
        pyspark.sql.SparkSession: Configured Spark session.
    """
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("NYC Taxi ETL")
        # Define external JARs for S3/MinIO and PostgreSQL connectivity
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.postgresql:postgresql:42.7.3"
        )

        # S3A configuration for MinIO compatibility
        .config("spark.hadoop.fs.s3a.endpoint", f"http://minio:{os.getenv('MINIO_API_PORT')}")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))

        # Essential settings to enable S3A object storage protocol over HDFS
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )