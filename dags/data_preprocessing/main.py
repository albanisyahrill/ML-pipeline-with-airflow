"""
DAG for preprocessing image data and storing processed data in MinIO for the ML pipeline.
"""
from airflow.decorators import dag
from data_preprocessing.utils.preprocess import image_preprocessing
from pendulum import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from tensorflow.keras.preprocessing.image import ImageDataGenerator
from airflow.models import Variable

@dag(
    start_date=datetime(2025, 7, 1),
    schedule=None,
    catchup=False
)
def data_preprocessing():
    """
    Defines the data preprocessing DAG, which preprocesses image data and triggers the training DAG.
    """
    datagen = ImageDataGenerator(rescale=1./255)
    image_size = (299, 299)

    # Preprocessing task
    data_preprocessing_task = image_preprocessing(datagen, image_size)

    # Trigger the training DAG after preprocessing
    training = TriggerDagRunOperator(
        task_id="training_trigger_dag",
        trigger_dag_id="train_eval_model",  # ID of the DAG to trigger
    )

    # Set dependencies: preprocessing must finish before training
    data_preprocessing_task >> training

data_preprocessing()