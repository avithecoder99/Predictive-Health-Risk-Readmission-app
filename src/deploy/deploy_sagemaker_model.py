import boto3
import sagemaker
from sagemaker import get_execution_role, Session
from sagemaker.model import Model
from datetime import datetime

# === CONFIG ===
s3_model_path = "s3://your-s3-bucket-name/output/xgboost-model/output/model.tar.gz"  # Update if needed
region = "us-east-1"
model_name = f"xgboost-readmission-{datetime.now().strftime('%Y%m%d%H%M%S')}"
endpoint_name = f"xgb-readmission-endpoint-{datetime.now().strftime('%Y%m%d%H%M%S')}"
role = "arn:aws:iam::YOUR_ACCOUNT_ID:role/service-role/SageMakerExecutionRole"  # Replace with your SageMaker role ARN

# === SageMaker Setup ===
boto_session = boto3.Session(region_name=region)
sagemaker_session = sagemaker.Session(boto_session=boto_session)

# === Use SageMaker Built-in XGBoost Container ===
container_uri = sagemaker.image_uris.retrieve(
    framework="xgboost",
    region=region,
    version="1.3-1"
)

# === Register Model ===
model = Model(
    image_uri=container_uri,
    model_data=s3_model_path,
    role=role,
    name=model_name,
    sagemaker_session=sagemaker_session
)

print(f" Model registered: {model_name}")

# === Deploy to Real-Time Endpoint ===
predictor = model.deploy(
    initial_instance_count=1,
    instance_type="ml.m5.large",  # Adjust as needed
    endpoint_name=endpoint_name
)

print(f" Model deployed to endpoint: {endpoint_name}")
