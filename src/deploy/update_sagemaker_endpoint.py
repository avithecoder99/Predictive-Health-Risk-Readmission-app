import boto3
import sagemaker
from sagemaker import get_execution_role
from sagemaker.model import Model
from datetime import datetime

# === CONFIG ===
region = "us-east-1"
role = "arn:aws:iam::YOUR_ACCOUNT_ID:role/service-role/SageMakerExecutionRole"  # UPDATE this
bucket = "your-s3-bucket-name"  # UPDATE this
model_artifact = f"s3://{bucket}/output/xgboost-model/output/model.tar.gz"
endpoint_name = "xgb-readmission-endpoint-main"  # Fixed endpoint name for reuse

# === Setup ===
boto_session = boto3.Session(region_name=region)
sagemaker_session = sagemaker.Session(boto_session=boto_session)

# === Use SageMaker Built-in XGBoost Image ===
container_uri = sagemaker.image_uris.retrieve(
    framework="xgboost",
    region=region,
    version="1.3-1"
)

# === Register New Model Version ===
model_name = f"xgb-model-{datetime.now().strftime('%Y%m%d%H%M%S')}"
model = Model(
    image_uri=container_uri,
    model_data=model_artifact,
    role=role,
    name=model_name,
    sagemaker_session=sagemaker_session
)

print(f" New model version registered: {model_name}")

# === Create New EndpointConfig ===
endpoint_config_name = f"{endpoint_name}-config-{datetime.now().strftime('%Y%m%d%H%M%S')}"
sagemaker_client = boto3.client("sagemaker", region_name=region)

sagemaker_client.create_endpoint_config(
    EndpointConfigName=endpoint_config_name,
    ProductionVariants=[
        {
            "VariantName": "AllTraffic",
            "ModelName": model_name,
            "InitialInstanceCount": 1,
            "InstanceType": "ml.m5.large"
        }
    ]
)

print(f" Created new endpoint config: {endpoint_config_name}")

# === Update Existing Endpoint ===
sagemaker_client.update_endpoint(
    EndpointName=endpoint_name,
    EndpointConfigName=endpoint_config_name
)

print(f" Endpoint '{endpoint_name}' updated to new model: {model_name}")
