"""
Data extraction and storage utilities for Airflow ML pipeline.
Handles bucket creation, dataset download, and validation in MinIO.
"""
from airflow.decorators import task
import kagglehub
import os
from helper.minio import get_minio_client, CustiomMinio
from airflow.exceptions import AirflowException

@task
def create_bucket():
    """
    Creates required MinIO buckets for raw, processed, and model data.
    Raises:
        AirflowException: If bucket creation fails.
    """
    try:
        minio_client = get_minio_client()
        bucket_name = ['raw-data', 'processed-data', 'model-results']
        # Create each bucket if it does not exist
        for bucket in bucket_name:
            if not minio_client.bucket_exists(bucket):
                minio_client.make_bucket(bucket)
    except Exception as e:
        # Raise an AirflowException if bucket creation fails
        raise AirflowException(f'Error when create bucket: {str(e)}')
                
@task
def extract_and_store_dataset():
    """
    Downloads dataset from KaggleHub, checks directory structure, and uploads train/test data to MinIO.
    Raises:
        AirflowException: If dataset path or directories are missing, or upload fails.
    """
    try:
        # Download the dataset from KaggleHub
        dataset_path = kagglehub.dataset_download('trainingdatapro/age-detection-human-faces-18-60-years')
        if not os.path.exists(dataset_path):
            raise AirflowException(f'Dataset path not found: {dataset_path}')

        print(f'Dataset path: {dataset_path}')

        # Define train and test directories
        train_dir = os.path.join(dataset_path, 'train')
        test_dir = os.path.join(dataset_path, 'test')

        # Check if train and test directories exist
        if not os.path.exists(train_dir):
            raise FileNotFoundError(f'Training directory not found: {train_dir}')
        if not os.path.exists(test_dir):
            raise FileNotFoundError(f'Test directory not found: {test_dir}')

        # Upload train and test data to MinIO
        CustiomMinio._put_image_dataset(train_dir, 'raw-data', 'train')
        CustiomMinio._put_image_dataset(test_dir, 'raw-data', 'test')

    except Exception as e:
        # Raise an AirflowException if upload fails
        raise AirflowException(f'Error when store dataset : {str(e)}')

@task
def validate_store():
    """
    Validates that data was stored successfully in MinIO by listing objects in the bucket.
    Raises:
        AirflowException: If no files are found in the bucket after storage.
    """
    try:
        minio_client = get_minio_client()
        bucket_name = 'raw-data'
                
        # List objects in the train and test directories in the bucket
        train_objects = list(minio_client.list_objects(bucket_name, prefix='train/'))
        test_objects = list(minio_client.list_objects(bucket_name, prefix='test/'))
            
        print(f'Found {len(train_objects)} training files')
        print(f'Found {len(test_objects)} test files')
                
        # Raise an error if no files are found
        if len(train_objects) == 0 or len(test_objects) == 0:
            raise AirflowException('No files found in bucket after Store')
                
    except Exception as e:
        # Raise an AirflowException if validation fails
        raise AirflowException(f'Error validating Store: {str(e)}')