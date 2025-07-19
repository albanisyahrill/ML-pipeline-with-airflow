# ML Pipeline With Airflow

A modular, production-ready machine learning pipeline orchestrated with Apache Airflow. This project covers the full ML lifecycle: **data collection, preprocessing, training, evaluation, and deployment** (with a Streamlit UI for inference), all managed via Airflow DAGs and using MinIO as the object storage backend.

---

## Project Structure

```
ML Pipeline With Airflow/
│
├── dags/
│   ├── data_collecting/
│   │   ├── __init__.py
│   │   ├── main.py                # Airflow DAG for data collection
│   │   └── utils/
│   │       └── extract.py         # Data extraction, MinIO upload, validation
│   ├── data_preprocessing/
│   │   ├── __init__.py
│   │   ├── main.py                # Airflow DAG for data preprocessing
│   │   └── utils/
│   │       └── preprocess.py      # Preprocessing logic, MinIO storage
│   ├── training_and_evaluation/
│   │   ├── __init__.py
│   │   ├── main.py                # Airflow DAG for training/evaluation
│   │   └── utils/
│   │       └── train_and_eval.py  # Model training, evaluation, MinIO model upload
│   ├── deployment/
│   │   ├── __init__.py
│   │   ├── main.py                # Airflow DAG for deployment
│   │   └── utils/
│   │       └── deploy.py          # Push Model To Hugging Face Hub
│   ├── helper/
│   │   ├── __init__.py
│   │   └── minio.py               # MinIO utility functions for all pipeline stages
│
├── docker-compose.yml             # Docker Compose for Airflow, MinIO, etc.
├── Dockerfile                     # Custom Dockerfile for Airflow image
├── requirements.txt               # Python dependencies (Airflow, ML, Streamlit, etc.)
├── start.sh                       # Startup script for the environment
```

---

## Pipeline Overview

### 1. Data Collection
- **DAG:** `dags/data_collecting/main.py`
- **Logic:** Downloads dataset (e.g., from KaggleHub), creates MinIO buckets, uploads raw data to MinIO, and validates storage.
- **Key Script:** `dags/data_collecting/utils/extract.py`

### 2. Data Preprocessing
- **DAG:** `dags/data_preprocessing/main.py`
- **Logic:** Downloads raw data from MinIO, preprocesses images (resizing, normalization), pickles processed data, and uploads it back to MinIO.
- **Key Script:** `dags/data_preprocessing/utils/preprocess.py`

### 3. Training & Evaluation
- **DAG:** `dags/training_and_evaluation/main.py`
- **Logic:** Downloads preprocessed data from MinIO, builds and trains a Keras model (InceptionV3), evaluates performance, and uploads the best model to MinIO.
- **Key Script:** `dags/training_and_evaluation/utils/train_and_eval.py`

### 4. Model Deployment (Hugging Face Hub)

- Once the deployment DAG runs, your trained model will be pushed to the Hugging Face Hub repository: `albanisyahril/model_ml_pipeline_with_airflow`.
- Ensure you have set the `HUGGINGFACE_TOKEN` environment variable in your Docker Compose or Airflow environment for authentication.
- The model will be available for download and sharing directly from Hugging Face Hub.

### 5. Helper Utilities
- **MinIO Helper:** `dags/helper/minio.py`  
  Centralizes all MinIO upload/download logic for datasets, pickled data, and model files.

---

## How the Pipeline Flows

1. **Data Collection DAG**  
   - Creates buckets, downloads and uploads raw data to MinIO.
   - Triggers the Data Preprocessing DAG.

2. **Data Preprocessing DAG**  
   - Downloads raw data, preprocesses it, uploads processed data to MinIO.
   - Triggers the Training & Evaluation DAG.

3. **Training & Evaluation DAG**  
   - Downloads processed data, trains and evaluates the model, uploads the best model to MinIO.
   - Triggers the Deployment DAG.

4. **Deployment DAG**  
   - Push model to Hugging Face Hub using the trained model from MinIO.

---

## Setup & Usage

### 1. Prerequisites
- Docker & Docker Compose
- Python 3.8+
- (Optional) Kaggle API credentials for dataset download

### 2. Installation

**Clone the repository:**
```bash
git clone <your-repo-url>
cd "ML Pipeline With Airflow"
```

**Install dependencies:**
- All dependencies are listed in `requirements.txt`.
- If using Docker, dependencies are installed automatically.

**Start the environment:**
```bash
docker-compose up -d
```
This will start Airflow, MinIO, and any other required services.

### 3. Running the Pipeline

- Access the Airflow UI (typically at [http://localhost:8080](http://localhost:8080)).
- Trigger the `data_collecting` DAG. The rest of the pipeline will be triggered automatically in sequence.

### 4. Push Model To Hugging Face Hub

- Once the deployment DAG runs, model will be push to Hugging Face Hub.

---

## Key Scripts Explained

### `dags/data_collecting/utils/extract.py`
- **create_bucket:** Creates MinIO buckets for raw, processed, and model data.
- **extract_and_store_dataset:** Downloads dataset, checks structure, uploads to MinIO.
- **validate_store:** Confirms data was uploaded successfully.

### `dags/data_preprocessing/utils/preprocess.py`
- **image_preprocessing:** Downloads raw data, preprocesses images, pickles and uploads processed data.

### `dags/training_and_evaluation/utils/train_and_eval.py`
- **training_evaluation_model:** Loads processed data, builds and trains a Keras model, evaluates, and uploads the best model to MinIO.

### `dags/deployment/utils/deploy.py`
- **login_huggingface_hub:** Authenticates to Hugging Face Hub using a token from the environment.
- **push_to_hf_hub:** Pushes the exported model folder to the Hugging Face Hub repository.
- **execute_push:** Airflow task that loads the model from MinIO, saves it to disk, and uploads it to Hugging Face Hub.

### `dags/helper/minio.py`
- Provides static methods for:
- Uploading/downloading datasets, pickled data, and model files to/from MinIO.
- Ensures all pipeline stages interact with MinIO in a consistent, robust way.

---

## Customizing the Pipeline

- **Change the model:** Edit `train_and_eval.py` to use a different Keras model.
- **Change preprocessing:** Edit `preprocess.py` for different image transformations.
- **Change dataset:** Update the dataset source in `extract.py`.

---

## Troubleshooting

- **MinIO connection issues:** Ensure MinIO is running and accessible from Airflow.
- **Hugging Face Hub deployment issues:** Ensure the `HUGGINGFACE_TOKEN` environment variable is set correctly and that you have the necessary permissions on the Hugging Face Hub repository.

---

## Contributing

1. Fork the repo and create your branch: `git checkout -b feature/your-feature`
2. Commit your changes: `git commit -am 'Add new feature'`
3. Push to the branch: `git push origin feature/your-feature`
4. Open a pull request

---

## License

MIT License

---

## Acknowledgements

- [Apache Airflow](https://airflow.apache.org/)
- [MinIO](https://min.io/)
- [Keras / TensorFlow](https://keras.io/)

---

**For questions or support, open an issue or contact the maintainer.** 