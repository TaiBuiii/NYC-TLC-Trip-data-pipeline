from airflow.sdk import task, dag
from src.ingestion.ingestion import ingest_data

@dag()
def etl_dag():

    @task.python
    def ingestion():
        ingest_data([2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026])

    ingestion()

etl_dag = etl_dag()
