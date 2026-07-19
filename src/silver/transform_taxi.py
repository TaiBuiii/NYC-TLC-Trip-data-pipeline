from pyspark.sql import functions as F
from src.utils.logger import get_logger

logger = get_logger(__name__)

def casting(df_raw):

    """
    This function converts the attributes into accurate format, and renames columns' name for better readablity
    Args:
        - df_raw: pyspark.sql.DataFrame before applying casting
    Returns: 
        = pyspark.sql.DataFrame: after being applied.
    """
    df = df_raw.select(
        F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_datetime"), 
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),
        F.col("trip_distance").cast("double"),
        F.col("PULocationID").cast("int").alias("pickup_location_id"),
        F.col("DOLocationID").cast("int").alias("dropoff_location_id"),
        F.col("passenger_count").cast("int"),
        F.col("payment_type").cast("int").alias("payment_id"),
        F.col("fare_amount").cast("double"),
        F.col("tip_amount").cast("double"),
        F.col("extra").cast("double"),
        F.col("total_amount").cast("double")
    )
    return df

def cleansing(df_raw, year, month):
    """
    This function focuses on filtering out negative numerical value, ensures dropoff time is after pick up time.
    Additionally, it also guarantees that there is no other year and month in this dataframe.
    Args:
        - df_raw: the dataframe to be cleaned
        - year: the year of this dataframe
        - month: the month of this dataframe
    Returns:
        - pyspark.sql.DataFrame: The cleaned DataFrame.
    """
    df = df_raw.filter(
        (F.col("trip_distance") > 0) &
        (F.col("total_amount") > 0) &
        (F.col("fare_amount") > 0) &
        (F.col("tip_amount") >= 0) &
        (F.col("dropoff_datetime") > F.col("pickup_datetime")) &
        (F.year("pickup_datetime") == year) &
        (F.month("pickup_datetime") == month)
    )
    return df


def feature_engineering(df_raw):
    """
    This function will converts trip_distance from miles to kilometer, calculate trip_duration and average trip, facilitating 
    analysis process.
    Args:
        - df_raw: pyspark.sql.DataFrame before applying feature engineering
    Returns: 
        = pyspark.sql.DataFrame: after being applied.
    """
    df = df_raw \
        .withColumn("trip_distance", F.round(F.col("trip_distance") * 1.60934,2)) \
        .withColumn("trip_duration",  F.round((F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60,2)) \
        .withColumn("average_speed" ,F.round(F.col("trip_distance") / (F.col("trip_duration")/60),2))
    return df

def filter_null(df_raw):
    df = df_raw.filter(
        (F.col("pickup_datetime").isNotNull()) &
        (F.col("dropoff_datetime").isNotNull()) &
        (F.col("pickup_location_id").isNotNull()) &
        (F.col("dropoff_location_id").isNotNull()) &
        (F.col("payment_id").isNotNull())
    )
    return df

def handle_outliers(df_raw):
    """
    Orchestrates the outliers handling process by looping through all the numerical values in the taxi data, and 
    calling filter_outlier() function.
    Args:
        - df_raw: pyspark.sql.DataFrame before filtering
    Returns:
        - pyspark.sql.DataFrame after applying 

    """
    def filter_outlier(df_raw, attr : str):
        """
        Eliminate all the outliers of the specifed field using IQR method
        Args:
            - df_raw: pyspark.sql.DataFrame before filtering outliers
            - attr: the numerical value name, which will be examined
        Returns:
            - pyspark.sql.DataFrame after applying filtering on one attribute
        """
            
        quantiles  = df_raw.stat.approxQuantile(attr, [0.25, 0.75],0.01)
        Q1, Q3 = quantiles[0], quantiles[1]
        IQR = Q3 - Q1
        LF = Q1 - 1.5 * IQR
        UF = Q3 + 1.5 * IQR
        
        return df_raw.filter(
            (F.col(attr) <= UF) &
            (F.col(attr) >= LF) 
        )
    
    numerical_attrs = ["trip_distance", "passenger_count", "fare_amount", "tip_amount", "extra", "total_amount", "trip_duration", "average_speed"]

    for attr in numerical_attrs:
        df_raw = filter_outlier(df_raw, attr)

    return df_raw

def transform_taxi(spark, years):
    """
    Orchestrates the transformation pipeline from Bronze to Silver layer for taxi trip data. Specifically,
    it iterates through 12 months of the specified years. For each month, it will perform casting, cleansing, 
    feature_engineering, filtering null, filtering outliers
    Args:
        spark (pyspark.sql.SparkSession): The active Spark session.
        years (list[int]): A list of years to process (e.g., [2025, 2026]).

    Returns:
        None: The function writes the transformed DataFrames directly to storage.
    """
    for year in years:
        for month in range(1, 13):
            try:
                bronze_path = f"s3a://bronze/{year}/{month:02d}/yellow_tripdata_{year}-{month:02d}.parquet"
                silver_path = f"s3a://silver/{year}/{month:02d}"

                df_raw = spark.read.parquet(bronze_path)
                df = casting(df_raw)
                df = filter_null(df)
                df = cleansing(df, year, month)
                df = feature_engineering(df)
                df = handle_outliers(df)

                df.write.mode("ignore").parquet(silver_path)
                logger.info(f"Transformed {year}-{month} -> {silver_path}")
            except Exception as e:
                logger.warning(f"Error occured transforming {year}-{month} -> {silver_path}: {e}")
                continue

    logger.info("Transform Taxi successfully")