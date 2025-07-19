from airflow.decorators import dag
from pendulum import datetime
from deployment.utils.deploy import execute_push

@dag(
    start_date=datetime(2025, 7, 1),
    schedule=None,
    catchup=False
)
def deployment():
    """
    Defines the deployment DAG, which push the model to hugging face hub.
    """
    execute_push()

deployment()