from tensorflow.keras.preprocessing.image import ImageDataGenerator
import tensorflow as tf
from airflow.exceptions import AirflowException
from airflow.decorators import task
from helper.minio import CustiomMinio
from tensorflow.keras.applications import InceptionV3
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Dense, GlobalAveragePooling2D
from sklearn.metrics import classification_report
import numpy as np
import pickle

@task
def training_evaluation_model(image_size):
    """
    Trains and evaluates an InceptionV3 model on preprocessed data from MinIO, then stores the best model to MinIO.

    Args:
        image_size (tuple): Input image size (height, width).
    Raises:
        AirflowException: If any error occurs during training, evaluation, or MinIO operations.
    """
    try:
        try:
            # Load preprocessed training and test data from MinIO
            X_train, y_train = CustiomMinio._get_pickle('processed-data', 'train_data')
            X_test, y_test = CustiomMinio._get_pickle('processed-data', 'test_data')

        except Exception as e:
            # Raise an AirflowException if data loading fails
            raise AirflowException(f'Error when get data preprocessed: {str(e)}')

        # Clear any previous Keras/TensorFlow session
        tf.keras.backend.clear_session()

        # Build the InceptionV3 base model
        base_model = InceptionV3(weights='imagenet', include_top=False, input_shape=image_size+(3,))
        # Set up a ModelCheckpoint callback to save the best model
        inceptionV3_checkpoint = tf.keras.callbacks.ModelCheckpoint('model.keras',
                monitor="val_loss", mode="min",
                save_best_only=True, verbose=1)

        # Freeze all layers in the base model
        for layer in base_model.layers:
            layer.trainable = False

        # Add global average pooling and dense output layer
        x = base_model.output
        x = GlobalAveragePooling2D()(x)
        output = Dense(5, activation='softmax')(x)

        # Create the final model
        model = Model(inputs=base_model.input, outputs=output)

        # Compile the model
        model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=0.001),
                            loss='categorical_crossentropy',
                            metrics=['accuracy'])

        # Train the model with validation and checkpointing
        model.fit(X_train, y_train,
                epochs = 20,
                validation_data = [X_test, y_test],
                callbacks=[inceptionV3_checkpoint])
        
        # Predict on the test set
        y_pred = model.predict(X_test)
        y_pred = np.argmax(y_pred, axis=1)
        y_true = np.argmax(y_test, axis=1)

        # Print classification report
        print(classification_report(y_true, y_pred))

        # Save the best model and upload to MinIO
        CustiomMinio._put_model_results('model.keras', 'model-results', 'model.keras')
    except Exception as e:
        # Raise an AirflowException for any error encountered
        raise AirflowException(f'An error has occured : {str(e)}')