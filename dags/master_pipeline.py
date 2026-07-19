from airflow.sdk import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import timedelta

DEFAULT_ARGS = {
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=60),
}

@dag(
    schedule="@monthly",
    catchup=False,
    default_args=DEFAULT_ARGS
)
def master_pipeline():
    """
    Master Orchestration Pipeline
    
    Execution flow:
    1. initialize storage (MinIO).
    2. initialize gold layer schema (Star Schema).
    3. runs ETL pipeline from Bronze to Gold layer.
    
    Attributes:
        schedule: @monthly
        catchup: False
    """
    init_storage = TriggerDagRunOperator(
        task_id="trigger_init_storage",
        trigger_dag_id="init_storage"
    )

    init_schema = TriggerDagRunOperator(
        task_id="trigger_init_schema",
        trigger_dag_id="init_star_schema"
    )

    etl_pipeline = TriggerDagRunOperator(
        task_id="trigger_etl",
        trigger_dag_id="etl_dag"
    )

    init_storage >> init_schema >> etl_pipeline

master_pipeline()