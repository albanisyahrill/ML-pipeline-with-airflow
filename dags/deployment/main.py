"""
DAG for deploying the trained ML model using Streamlit UI.
"""
from airflow.decorators import dag
from pendulum import datetime
from deployment.utils.deploy import create_streamlit_ui
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator

@dag(
    start_date=datetime(2025, 7, 1),
    schedule=None,
    catchup=False
)
def deployment():
    """
    Defines the deployment DAG, which runs the Streamlit UI for model inference.
    """
    run_streamlit = BashOperator(
    task_id='run_streamlit_bash',
    bash_command='streamlit run dags/deployment/utils/deploy.py --server.port 8501',
    cwd='/opt/airflow',
    env={
        "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
        "PYTHONPATH": "/opt/airflow"
        }
    )
    # Only one task: run the Streamlit UI
    run_streamlit

deployment()