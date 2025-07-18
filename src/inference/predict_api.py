from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
import boto3
import uuid
import json
from datetime import datetime
import logging
import watchtower
from src.post_prediction.store_to_sql import store_prediction_to_sql

# === CONFIG ===
S3_BUCKET = "your-s3-bucket-name"
LOG_PREFIX = "inference_logs/"
ENDPOINT_NAME = "xgb-readmission-endpoint-20240718143000"  # Replace with your SageMaker endpoint
REGION = "us-east-1"

# === SageMaker and CloudWatch clients ===
sm_runtime = boto3.client("sagemaker-runtime", region_name=REGION)
cloudwatch = boto3.client("cloudwatch", region_name=REGION)

# === FastAPI App ===
app = FastAPI(title="Healthcare Risk Prediction API (SageMaker)")

# === CloudWatch Logging Setup ===
LOG_GROUP = "HealthcarePredictionLogs"
logger = logging.getLogger("healthcare-api")
logger.setLevel(logging.INFO)
logger.addHandler(watchtower.CloudWatchLogHandler(log_group=LOG_GROUP))

# === Input Schema ===
class PatientInput(BaseModel):
    patient_id: str
    age: int
    gender: str
    blood_pressure: str
    heart_rate: int
    cholesterol: float
    blood_sugar: float
    oxygen_saturation: float
    temperature: float

# === Preprocess Input ===
def preprocess(data: PatientInput):
    df = pd.DataFrame([data.dict()])
    df = pd.get_dummies(df, columns=["gender", "blood_pressure"], drop_first=True)

    for col in ["gender_male", "blood_pressure_normal"]:
        if col not in df.columns:
            df[col] = 0

    return df[[
        "age", "heart_rate", "cholesterol", "blood_sugar",
        "oxygen_saturation", "temperature", "gender_male", "blood_pressure_normal"
    ]]

# === Log to S3 ===
def log_to_s3(data: dict, result: dict):
    s3 = boto3.client("s3")
    timestamp = datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")
    log_id = f"{data['patient_id']}_{timestamp}_{uuid.uuid4().hex[:6]}"
    s3.put_object(Bucket=S3_BUCKET, Key=f"{LOG_PREFIX}inputs/{log_id}.json", Body=json.dumps(data))
    s3.put_object(Bucket=S3_BUCKET, Key=f"{LOG_PREFIX}outputs/{log_id}.json", Body=json.dumps(result))
    logger.info(f" Input/output logged to S3 for {data['patient_id']}")

# === Prediction Endpoint ===
@app.post("/predict")
async def predict(input: PatientInput):
    data_dict = input.dict()
    logger.info(f"[Request Received] Patient ID: {data_dict['patient_id']}")

    try:
        X = preprocess(input)
        csv_payload = X.to_csv(index=False, header=False)

        start_time = datetime.utcnow()

        # Call SageMaker Endpoint
        response = sm_runtime.invoke_endpoint(
            EndpointName=ENDPOINT_NAME,
            ContentType="text/csv",
            Body=csv_payload
        )

        result_payload = response["Body"].read().decode("utf-8")
        y_proba = float(result_payload.strip())
        y_pred = int(y_proba > 0.5)

        latency = (datetime.utcnow() - start_time).total_seconds() * 1000  # ms

        # Prepare result
        result = {
            "readmitted_prediction": y_pred,
            "readmitted_probability": round(y_proba, 4),
            "timestamp": datetime.utcnow().isoformat()
        }

        # Logging
        log_to_s3(data_dict, result)
        store_prediction_to_sql(data_dict, result)
        logger.info(f"[Prediction Result] ID: {data_dict['patient_id']} y={y_pred} prob={y_proba:.4f}")

        # CloudWatch Custom Metrics
        cloudwatch.put_metric_data(
            Namespace="HealthcarePrediction",
            MetricData=[
                {
                    'MetricName': 'SuccessfulPredictions',
                    'Dimensions': [{'Name': 'ModelVersion', 'Value': 'xgboost-v1'}],
                    'Unit': 'Count',
                    'Value': 1
                },
                {
                    'MetricName': 'PredictionLatencyMs',
                    'Dimensions': [{'Name': 'ModelVersion', 'Value': 'xgboost-v1'}],
                    'Unit': 'Milliseconds',
                    'Value': latency
                }
            ]
        )

        return {
            "patient_id": data_dict["patient_id"],
            "prediction": y_pred,
            "probability": round(y_proba, 4),
            "message": " Prediction complete (via SageMaker)"
        }

    except Exception as e:
        logger.error(f"[ERROR] Prediction failed for {data_dict['patient_id']}", exc_info=True)
        cloudwatch.put_metric_data(
            Namespace="HealthcarePrediction",
            MetricData=[{
                'MetricName': 'PredictionFailures',
                'Unit': 'Count',
                'Value': 1
            }]
        )
        return {
            "error": "Prediction failed",
            "details": str(e)
        }
