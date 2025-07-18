"""
Data preprocessing utilities for Airflow ML pipeline.
Handles image data loading, preprocessing, and storage to MinIO.
"""
from airflow.exceptions import AirflowException
from airflow.decorators import task
from helper.minio import CustiomMinio
import pickle

@task
def image_preprocessing(datagen, image_size, batch_size=32):
    """
    Loads image data from MinIO, preprocesses it, and stores processed data as pickled numpy arrays.

    Args:
        datagen (ImageDataGenerator): Keras image data generator instance.
        image_size (tuple): Target image size (height, width).
        batch_size (int, optional): Batch size for data generator. Defaults to 32.
    Raises:
        AirflowException: If any error occurs during processing or MinIO operations.
    """
    try:
        # Download training and test image datasets from MinIO
        try:
            train_data = CustiomMinio._get_image_dataset('raw-data', 'train', 'dataset')
            test_data = CustiomMinio._get_image_dataset('raw-data', 'test', 'dataset')
            print(train_data)
            print(test_data)
        except Exception as e:
            raise AirflowException(f'Error when get dataset: {str(e)}')

        # Create data generators for training and test sets
        train_data_gen = datagen.flow_from_directory(
            directory=train_data,
            target_size=image_size,
            batch_size=batch_size,
            class_mode='categorical'
        )
        test_data_gen = datagen.flow_from_directory(
            directory=test_data,
            target_size=image_size,
            batch_size=batch_size,
            class_mode='categorical'
        )

        # Get one batch of data from each generator (or loop for all data if needed)
        X_train, y_train = next(train_data_gen)
        X_test, y_test = next(test_data_gen)

        # Pickle the arrays (numpy arrays) for storage
        train_pickle_data = pickle.dumps((X_train, y_train))
        test_pickle_data = pickle.dumps((X_test, y_test))

        # Upload pickled data to MinIO
        CustiomMinio._put_pickle_file(train_pickle_data, 'processed-data', 'train_data')
        CustiomMinio._put_pickle_file(test_pickle_data, 'processed-data', 'test_data')

    except Exception as e:
        # Raise an AirflowException for any error encountered
        raise AirflowException(f'An error has occured : {str(e)}')


        