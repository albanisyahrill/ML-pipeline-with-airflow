from airflow.exceptions import AirflowException
from airflow.decorators import task
from helper.minio import CustiomMinio
import pickle
import numpy as np

def generator_to_arrays(generator):
    """
    Converts all batches from a Keras ImageDataGenerator iterator into full numpy arrays.

    Iterates through the entire generator (using its __len__ and __getitem__ methods),
    collecting all image and label batches, and concatenates them into single numpy arrays
    for X (images) and y (labels).

    Args:
        generator (DirectoryIterator): A Keras ImageDataGenerator iterator, such as returned by flow_from_directory.

    Returns:
        tuple: (X, y) where X is a numpy array of all images, and y is a numpy array of all labels.
    """
    X_list, y_list = [], []
    # Iterate through all batches in the generator
    for i in range(len(generator)):
        X, y = generator[i]
        X_list.append(X)
        y_list.append(y)
        
    return np.concatenate(X_list), np.concatenate(y_list)

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
            class_mode='categorical',
            shuffle=False
        )
        
        print("Train class indices:", train_data_gen.class_indices)
        print("Test class indices:", test_data_gen.class_indices)

        # Convert all batches from the train dan test data generator into full numpy arrays
        X_train, y_train = generator_to_arrays(train_data_gen)
        X_test, y_test = generator_to_arrays(test_data_gen)


        print("y_train shape:", y_train.shape)
        print("y_test shape:", y_test.shape)
        print("Sample y_train:", y_train[:5])

        # Pickle the arrays (numpy arrays) for storage
        train_pickle_data = pickle.dumps((X_train, y_train))
        test_pickle_data = pickle.dumps((X_test, y_test))

        # Upload pickled data to MinIO
        CustiomMinio._put_pickle_file(train_pickle_data, 'processed-data', 'train_data')
        CustiomMinio._put_pickle_file(test_pickle_data, 'processed-data', 'test_data')

    except Exception as e:
        # Raise an AirflowException for any error encountered
        raise AirflowException(f'An error has occured : {str(e)}')


        