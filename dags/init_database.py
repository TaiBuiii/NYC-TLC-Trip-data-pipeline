from pathlib import Path

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from src.utils.logger import get_logger
from datetime import timedelta
logger = get_logger(__name__)

SQL_FILES = [
    "gold_schema.sql",
    "dim_location.sql",
    "dim_payment.sql",
    "fact_trip.sql",
]
@dag(
    schedule=None,
    catchup=False,
)
def init_star_schema():
    """
    Airflow DAG to initialize the Gold layer star schema in PostgreSQL.
    Reads SQL files from the local directory and executes them sequentially.
    """
    @task(retries=3, retry_delay=timedelta(minutes=5))
    def create_schema():
        """
        Establishes a connection to PostgreSQL via airflow UI and executes setup scripts.
        """
        hook = PostgresHook(postgres_conn_id="star_schema")
        engine = hook.get_sqlalchemy_engine()

        sql_dir = Path("/opt/airflow/sql")

        with engine.begin() as conn:
            for file in SQL_FILES:
                try: 
                    sql = (sql_dir / file).read_text(encoding="utf-8")
                    conn.exec_driver_sql(sql)

                    logger.info(f"Execute {file} successfully")
                except Exception as e:
                    logger.error(f"Error creating schema: {e}")

    create_schema()


init_star_schema()