import boto3
import os

BUCKET = "your-bucket-name"
S3_MODEL_PATH = "model_artifacts/xgb-readmission-train/output/model.tar.gz"
LOCAL_MODEL_DIR = "models/"
LOCAL_TAR_FILE = os.path.join(LOCAL_MODEL_DIR, "model.tar.gz")

# Download from S3
s3 = boto3.client("s3")
s3.download_file(BUCKET, S3_MODEL_PATH, LOCAL_TAR_FILE)
print(" Downloaded model.tar.gz to models/")

# Extract .model file
import tarfile
with tarfile.open(LOCAL_TAR_FILE, "r:gz") as tar:
    tar.extractall(path=LOCAL_MODEL_DIR)

print(" Extracted XGBoost model to models/")
