from airflow.hooks.base import BaseHook
from minio import Minio
import os
from airflow.exceptions import AirflowException
import pickle
from io import BytesIO
from tensorflow.keras.models import load_model

def get_minio_client():
    minio = BaseHook.get_connection('minio')
    minio_client = Minio(
        endpoint=minio.extra_dejson.get('endpoint_url'),
        access_key=minio.login,
        secret_key=minio.password,
        secure=False,
    )

    return minio_client
    
class CustiomMinio:
    """
    Utility class for interacting with MinIO for dataset and model storage in Airflow pipelines.
    """
    @staticmethod
    def _put_image_dataset(folder_path, bucket_name, object_name):
        """
        Uploads all files from a local folder to a MinIO bucket under a given object prefix.

        Args:
            folder_path (str): Local directory path to upload.
            bucket_name (str): MinIO bucket name.
            object_name (str): Prefix for objects in the bucket.
        Raises:
            FileNotFoundError: If the folder does not exist.
            NotADirectoryError: If the path is not a directory.
            AirflowException: If upload fails.
        """
        minio_client = get_minio_client()

        if not os.path.exists(folder_path):
            raise FileNotFoundError(f'Directory not found: {folder_path}')
                
        if not os.path.isdir(folder_path):
            raise NotADirectoryError(f'Path is not a directory: {folder_path}')

        uploaded_files = 0
        skipped_files = 0

        print(f'Scanning directory: {folder_path}')
        
        for root, dirs, files in os.walk(folder_path):
            print(f'Processing folder: {root}')
            print(f'Files found: {len(files)}')

            for file in files:
                try:
                    file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(file_path, folder_path).replace('\\', '/')
                    print(f'Uploading file: {relative_path}')

                    if not os.path.isfile(file_path):
                        print(f'⚠️ Skipping non-file: {file_path}')
                        skipped_files += 1
                        continue

                    full_object_path = f'{object_name}/{relative_path}'

                    minio_client.fput_object(
                        bucket_name=bucket_name ,
                        object_name=full_object_path,
                        file_path=file_path,
                    )

                    uploaded_files += 1
                except Exception as e:
                    raise AirflowException(f'Error when upload file: {str(e)}')

    def _put_pickle_file(file, bucket_name, object_name):
        """
        Uploads a pickled data object to MinIO.

        Args:
            file (bytes): Pickled data as bytes.
            bucket_name (str): MinIO bucket name.
            object_name (str): Object name in the bucket.
        """
        pickle_buffer = BytesIO(file)

        minio_client = get_minio_client()
        minio_client.put_object(
            bucket_name = bucket_name,
            object_name = object_name,
            data = pickle_buffer,
            length = len(file),
        )

    @staticmethod
    def _get_image_dataset(bucket_name, object_name, destination_path):
        """
        Downloads a dataset from MinIO to a local directory.

        Args:
            bucket_name (str): MinIO bucket name.
            object_name (str): Prefix for objects in the bucket.
            destination_path (str): Local directory to store the dataset.
        Returns:
            str: Local path to the downloaded dataset.
        """
        minio_client = get_minio_client()

        if not os.path.exists(destination_path):
            os.makedirs(destination_path)

        getting_files = 0

        print(f'Fetching dataset from bucket: {bucket_name}, prefix: {object_name}/')

        try:
            objects = minio_client.list_objects(
                bucket_name=bucket_name,
                prefix=f'{object_name}/',
                recursive=True,
            )

            if not objects:
                raise AirflowException(f"No objects found in {bucket_name}/{object_name}")

            objects_list = list(objects)

            for obj in objects_list:
                try:
                    relative_path = obj.object_name
                    local_file_path = os.path.join(destination_path, relative_path)

                    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                    print(f'Downloading: {obj.object_name} {relative_path}')

                    minio_client.fget_object(
                        bucket_name=bucket_name,
                        object_name=obj.object_name,
                        file_path=local_file_path,
                    )

                    getting_files += 1  
                    print(f'Downloaded: {relative_path}')

                except Exception as e:
                    print(f'Error downloading {obj.object_name}: {str(e)}')
                    skipped_files += 1
                    continue

        except Exception as e:
            raise AirflowException(f'Error when get dataset: {str(e)}')
        
        print(f'Total downloaded: {getting_files}')
        
        local_folder_path = os.path.join(destination_path, object_name)
        return local_folder_path

    @staticmethod
    def _get_pickle(bucket_name, object_name):
        """
        Downloads and unpickles a data object from MinIO.

        Args:
            bucket_name (str): MinIO bucket name.
            object_name (str): Object name in the bucket.
        Returns:
            Any: Unpickled data object.
        """
        minio_client = get_minio_client()
        data = minio_client.get_object(
            bucket_name = bucket_name,
            object_name = object_name
        )

        loaded_data = pickle.load(data)

        return loaded_data

    @staticmethod
    def _put_model_results(model_path, bucket_name, object_name):
        """
        Uploads a model file (e.g., .keras or .h5) to MinIO.

        Args:
            model_path (str): Path to the model file on disk.
            bucket_name (str): MinIO bucket name.
            object_name (str): Object name in the bucket.
        """
        with open(model_path, 'rb') as f:
            data = f.read()
        model_buffer = BytesIO(data)

        minio_client = get_minio_client()
        minio_client.put_object(
            bucket_name = bucket_name,
            object_name = object_name,
            data = model_buffer,
            length = len(data),
        )

    @staticmethod
    def _get_model_results(bucket_name, object_name, download_path):
        """
        Downloads a model file from MinIO, writes it to disk, and loads it with Keras.

        Args:
            bucket_name (str): MinIO bucket name.
            object_name (str): Object name in the bucket.
            download_path (str): Local path to save the downloaded model file.
        Returns:
            keras.Model: Loaded Keras model.
        """
        minio_client = get_minio_client()
        model = minio_client.get_object(
            bucket_name=bucket_name,
            object_name=object_name,
        )

        model_bytes = model.read()
        with open(download_path, 'wb') as f:
            f.write(model_bytes)
        model = load_model(download_path)

        return model