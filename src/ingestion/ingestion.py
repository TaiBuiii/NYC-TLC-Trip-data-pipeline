import requests
from src.utils.db_connection import get_minio_client 
from src.utils.logger import get_logger

logger = get_logger(__name__)

def check_object_existence(s3_client, bucket, key) -> bool:
    """
    This function check if the file is already exist in MinIO to avoid ingesting 
    duplicate file.
    """
    try:
        s3_client.head_object(
            Bucket = bucket,
            Key = key
        )
        return True
    except:
        return False

def generate_file_info(year, month)->dict:
    """
    Generate the info of parquet file, used for ingestion.
    """
    return {
            "file_name": f"yellow_tripdata_{year}-{month:02d}.parquet",
            "url": f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet",
            "s3_key": f"{year}/{month:02d}/yellow_tripdata_{year}-{month:02d}.parquet"
            }

def ingest_data(years):
    """
    Ingest all data from 2009 to 2026 on NYC Taxi, and store into bronze of MinIO.
    """

    s3_client = get_minio_client()    

        
    for year in years:
        for month in range(1, 13):
            
            file_info = generate_file_info(year, month)

            try:
                if check_object_existence(s3_client, "bronze", file_info["s3_key"]):
                    logger.warning(f"{file_info["s3_key"]} is already exist, skip")
                    continue

                with requests.get(file_info["url"], stream= True) as r:
                    if r.status_code == 200:                        
                        s3_client.upload_fileobj(
                            Bucket="bronze",
                            Key=file_info["s3_key"],
                            Fileobj=r.raw
                        )
                        logger.info(f"Ingested {file_info["file_name"]} to {file_info["s3_key"]}")
                    else:
                        logger.error(f"Failed Ingesting {file_info["file_name"]} to {file_info["s3_key"]}")
                        continue
                        
            except Exception as e:
                raise 

