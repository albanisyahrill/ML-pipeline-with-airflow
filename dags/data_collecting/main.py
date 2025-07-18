"""
DAG for collecting data and storing it in MinIO for the ML pipeline.
"""
from airflow.decorators import dag, task_group
from pendulum import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from data_collecting.utils.extract import create_bucket, extract_and_store_dataset, validate_store

@dag(
    start_date=datetime(2025, 7, 1),
    schedule='@once',
    catchup=False
)
def data_collecting():
    """
    Defines the data collecting DAG, which creates buckets, extracts datasets, and validates storage in MinIO.
    """
    @task_group
    def data_collecting_group():
        """
        Task group for bucket creation, dataset extraction, and validation.
        """
        # Chain: create_bucket -> extract_and_store_dataset -> validate_store
        create_bucket() >> extract_and_store_dataset() >> validate_store()

    # Trigger the preprocessing DAG after data collection
    preprocess = TriggerDagRunOperator(
        task_id="preprocess_trigger_dag",
        trigger_dag_id="data_preprocessing",  # ID of the DAG to trigger
    )

    # Set dependencies: data collection group must finish before triggering preprocess
    data_collecting_group() >> preprocess

data_collecting()