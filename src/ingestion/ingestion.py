import requests
from src.utils.db_connection import get_minio_client 
from src.utils.logger import get_logger

logger = get_logger(__name__)

BUCKET_NAME = "bronze"

def check_object_existence(s3_client, bucket, s3_key) -> bool:
    """
    Check if the file already exists in MinIO to avoid duplicate ingestion.
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=s3_key)
        return True
    except Exception:
        return False

def generate_file_info(year, month) -> dict:
    """
    Generate the info of parquet file, used for ingestion.
    """
    return {
        "url": f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet",
        "s3_key": f"{year}/{month:02d}/yellow_tripdata_{year}-{month:02d}.parquet"
    }

def load_data(s3_client, bucket, url, s3_key):
    """
    Download data and stream load into MinIO.
    """
    with requests.get(url, stream=True, timeout=30) as r:
        if r.status_code == 200:
            s3_client.upload_fileobj(
                Bucket=bucket,  
                Key=s3_key,
                Fileobj=r.raw
            )
            logger.info(f"Successfully ingested {s3_key}")
        else:
            logger.error(f"Failed Ingesting {s3_key}")


def process_and_load(s3_client, url, s3_key):
    """
    Check if the data already exists before loading data
    """
    if check_object_existence(s3_client, BUCKET_NAME, s3_key):
        logger.warning(f"{s3_key} already exists, skipping")
        return 
    
    logger.info(f"Ingesting {s3_key}")
    load_data(s3_client, BUCKET_NAME, url, s3_key)

  

def ingest_data(years):
    """
    Ingest all data from 2009 to 2026 on NYC Taxi, and store into bronze of MinIO.
    """
    s3_client = get_minio_client()    

    zone_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    zone_key = "misc/taxi_zone_lookup.csv"

    try:
        process_and_load(s3_client, url=zone_url, s3_key=zone_key)
    except Exception as e:
        logger.error(f"Error ingesting zone lookup: {str(e)}")

    for year in years:
        for month in range(1, 13):
            file_info = generate_file_info(year, month)
            try:
                process_and_load(
                        s3_client=s3_client, 
                        url=file_info["url"], 
                        s3_key=file_info["s3_key"]
                    )
            except Exception as e:
                logger.error(f"Error ingesting {year}-{month:02d}: {str(e)}")
                continue
