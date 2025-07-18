"""
DAG for training and evaluating the ML model, then triggering deployment.
"""
from airflow.decorators import dag
from pendulum import datetime
from training_and_evaluation.utils.train_and_eval import training_evaluation_model
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable

@dag(
    start_date=datetime(2025, 7, 1),
    schedule=None,
    catchup=False
)
def train_eval_model():
    """
    Defines the training and evaluation DAG, which trains the model and triggers deployment.
    """
    image_size = (299, 299)

    # Training and evaluation task
    train_and_eval_model = training_evaluation_model(image_size)

    # Trigger the deployment DAG after training and evaluation
    deployment = TriggerDagRunOperator(
        task_id="deployment_trigger_dag",
        trigger_dag_id="deployment",  # ID of the DAG to trigger
    )

    # Set dependencies: training/evaluation must finish before deployment
    train_and_eval_model >> deployment

train_eval_model()