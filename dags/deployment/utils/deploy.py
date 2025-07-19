from helper.minio import CustiomMinio
from airflow.exceptions import AirflowException
import os
from huggingface_hub import login, HfApi, create_repo
from airflow.decorators import task


def login_huggingface_hub():
    """
    Authenticate to Hugging Face Hub using a token from the environment variable 'HUGGINGFACE_TOKEN'.
    Raises an error if the token is not set.
    """
    token = os.getenv("HUGGINGFACE_TOKEN")
    if not token:
        raise ValueError("HUGGINGFACE_TOKEN environment variable is not set.")

    login(token=token)

def load_model():
    """
    Retrieve the trained Keras model from MinIO and save it to disk.

    Returns:
        str: Path to the directory containing the saved model file.
    """
    model = model = CustiomMinio._get_model_results('model-results', 'model.keras', 'model.keras')
    # Create a temporary directory to store the model file
    model_dir = "/tmp/model_folder"
    os.makedirs(model_dir, exist_ok=True)

    # Save the Keras model to the directory
    model_path = os.path.join(model_dir, 'model.keras')
    model.save(model_path)

    return model_dir


def push_to_hf_hub(model_folder_path):
    """
    Push the exported model folder to the Hugging Face Hub repository.

    Args:
        model_folder_path (str): Path to the folder containing the saved model file(s).
    """
    repo_id = 'albanisyahril/model_ml_pipeline_with_airflow'
    api = HfApi()
    # Create the repo if it doesn't exist
    create_repo(repo_id, exist_ok=True)
    # Upload the entire folder to the repo
    api.upload_folder(
        folder_path=model_folder_path,
        repo_id=repo_id,
        commit_message='Push tensorflow model from airflow'
    )


@task
def execute_push():
    """
    Airflow task to authenticate, retrieve the trained model from MinIO, save it to disk,
    and upload it to Hugging Face Hub.
    """
    try:
        # Authenticate to Hugging Face Hub
        login_huggingface_hub()

        # Save the Keras model to disk in the temporary directory and get the model directory
        model_dir = load_model()

        # Upload the model directory to Hugging Face Hub
        push_to_hf_hub(model_dir)

        print('Model has been pushed to Hugging Face Hub')
    except Exception as e:
        # Raise an AirflowException for any error during the process
        raise AirflowException(f'Error when push model: {str(e)}')