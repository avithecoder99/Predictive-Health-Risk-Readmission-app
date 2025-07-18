import sagemaker
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.processing import ProcessingInput, ProcessingOutput
import boto3
import os
from datetime import datetime

# === CONFIGURATION ===
S3_INPUT_PATH = "s3://your-bucket-name/raw_data/patient_data.csv"
S3_OUTPUT_PATH = "s3://your-bucket-name/preprocessed"
ROLE_ARN = "arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/SageMakerExecutionRole"
BUCKET = "your-bucket-name"
REGION = "us-east-1"

# Initialize SageMaker session and processor
sagemaker_session = sagemaker.Session()
sklearn_processor = SKLearnProcessor(
    framework_version="0.23-1",
    role=ROLE_ARN,
    instance_type="ml.m5.large",
    instance_count=1,
    base_job_name="preprocess-job",
    sagemaker_session=sagemaker_session
)

# === SCRIPT THAT RUNS INSIDE THE CONTAINER ===
PROCESSING_SCRIPT = "src/modeling/sklearn_preprocess_script.py"

# === START JOB ===
def run_processing_job():
    timestamp = datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")
    sklearn_processor.run(
        code=PROCESSING_SCRIPT,
        inputs=[
            ProcessingInput(
                source=S3_INPUT_PATH,
                destination="/opt/ml/processing/input"
            )
        ],
        outputs=[
            ProcessingOutput(
                source="/opt/ml/processing/output/train",
                destination=f"{S3_OUTPUT_PATH}/train/{timestamp}"
            ),
            ProcessingOutput(
                source="/opt/ml/processing/output/test",
                destination=f"{S3_OUTPUT_PATH}/test/{timestamp}"
            )
        ]
    )
    print(" SageMaker preprocessing job launched.")

if __name__ == "__main__":
    run_processing_job()
