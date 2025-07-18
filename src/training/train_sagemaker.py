import sagemaker
from sagemaker.estimator import Estimator
from sagemaker.inputs import TrainingInput
import boto3
import os
from datetime import datetime

# === CONFIG ===
ROLE_ARN = "arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/SageMakerExecutionRole"
BUCKET = "your-bucket-name"
REGION = "us-east-1"
S3_OUTPUT_PATH = f"s3://{BUCKET}/model_artifacts/"
S3_TRAIN_PATH = f"s3://{BUCKET}/preprocessed/train/"

# Create SageMaker session
session = sagemaker.Session()
xgb_container_uri = sagemaker.image_uris.retrieve("xgboost", REGION, version="1.5-1")

# === TRAINING ESTIMATOR ===
estimator = Estimator(
    image_uri=xgb_container_uri,
    role=ROLE_ARN,
    instance_count=1,
    instance_type="ml.m5.large",
    output_path=S3_OUTPUT_PATH,
    sagemaker_session=session,
    base_job_name="xgb-readmission-train"
)

# Set hyperparameters
estimator.set_hyperparameters(
    objective="binary:logistic",
    num_round=100,
    max_depth=5,
    eta=0.2,
    subsample=0.8,
    colsample_bytree=0.8,
    eval_metric="logloss"
)

# Input format
train_input = TrainingInput(
    s3_data=S3_TRAIN_PATH,
    content_type="csv"
)

# Launch training job
def run_training_job():
    estimator.fit({"train": train_input})
    print(" SageMaker training job launched and model saved to S3.")

if __name__ == "__main__":
    run_training_job()
