from tensorflow.keras.applications.inception_v3 import preprocess_input
import streamlit as st
from PIL import Image
import json
from helper.minio import CustiomMinio
from tensorflow.keras.preprocessing.image import load_img, img_to_array
from airflow.models import Variable
from tensorflow.keras.applications.inception_v3 import preprocess_input
from airflow.models import Variable

def preprocess_image(image, image_size, preprocess_input):
    """
    Preprocesses an input image for model prediction.

    Args:
        image (PIL.Image): The input image to preprocess.
        image_size (tuple): The target size (height, width) for resizing the image.
        preprocess_input (callable): Function to further preprocess the image for the model.

    Returns:
        np.ndarray: The preprocessed image as a numpy array, ready for model input.
    """
    image = load_img(image, target_size=image_size)
    img = img_to_array(image)
    img = np.expand_dims(img, axis=0)
    preprocess_img = preprocess_input(img)

    return preprocess_img

def predict_image(model, uploaded_file, image_size, class_indices, preprocess_image, preprocess_input):
    """
    Predicts the class of an uploaded image using the provided model and preprocessing functions.

    Args:
        model (keras.Model): The trained Keras model for prediction.
        uploaded_file (UploadedFile): The uploaded image file from Streamlit.
        image_size (tuple): The target size (height, width) for resizing the image.
        class_indices (dict): Mapping of class names to indices.
        preprocess_image (callable): Function to preprocess the image.
        preprocess_input (callable): Function to further preprocess the image for the model.

    Returns:
        str: The predicted class label for the input image.
    """
    preprocess_img = preprocess_image(uploaded_file, image_size)
    predictions = model.predict(preprocess_img)
    predicted_class_idx = np.argmax(predictions)
    labels = list(class_indices)
    predicted_class_label = labels[predicted_class_idx]
        
    return predicted_class_label

def create_streamlit_ui():
    """
    Loads the trained model from MinIO and runs a Streamlit UI for image upload and inference.

    This function sets up the Streamlit interface, handles image upload, runs prediction,
    and displays the result to the user.
    """
    image_size = (299, 299)
    class_indices = Variable.get('class_indices')
    # Load the trained model from MinIO
    model = CustiomMinio._get_model_results('model-results', 'model.keras', 'downloaded_model.keras')

    # Set the page title
    st.title('Age Classification Based on Face')

    # Add a description
    st.write('Upload a face photo to predict the age group')

    # File uploader for image input
    uploaded_file = st.file_uploader(
        "Choose an image...",
        type=['jpg', 'jpeg', 'png']
    )

    if uploaded_file is not None:
        try:
            # Display the uploaded image
            image = Image.open(uploaded_file)
            st.image(image, caption='Uploaded Image', width=300, use_container_width=True)

            # Perform prediction
            with st.spinner('Predicting...'):
                predicted_image = predict_image(model, uploaded_file, image_size, class_indices, preprocess_image, preprocess_input)

            # Display the result
            st.success('Prediction successful!')
            st.write(f'This is: {predicted_image}')

        except Exception as e:
            # Display error message if prediction fails
            st.error(f'Error: {str(e)}')
