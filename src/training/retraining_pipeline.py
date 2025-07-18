import boto3
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from io import StringIO
import os
import sagemaker
from sagemaker.inputs import TrainingInput
from sagemaker.estimator import Estimator

# === CONFIG ===
DB_TYPE = "mysql"  # or "postgresql"
USER = "admin"
PASSWORD = "admin123"
HOST = "localhost"
PORT = "3306"
DB_NAME = "hospital_db"
REGION = "us-east-1"
BUCKET = "your-s3-bucket-name"
ROLE_ARN = "arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/SageMakerExecutionRole"
RETRAIN_PREFIX = "retraining/"
ARTIFACT_PATH = f"s3://{BUCKET}/retraining_output/"

def get_engine():
    if DB_TYPE == "mysql":
        return create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}")
    elif DB_TYPE == "postgresql":
        return create_engine(f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}")
    else:
        raise ValueError("Unsupported DB type")

def check_drift(engine):
    patient_data_count = pd.read_sql("SELECT COUNT(*) AS count FROM patient_data;", con=engine)['count'].iloc[0]
    predictions_count = pd.read_sql("SELECT COUNT(*) AS count FROM predictions_log;", con=engine)['count'].iloc[0]
    
    print(f" Original data: {patient_data_count} rows")
    print(f" New predicted data: {predictions_count} rows")

    if predictions_count >= 0.1 * patient_data_count:
        print(" Drift threshold exceeded (≥10%). Retraining needed.")
        return True
    else:
        print(" No significant drift detected.")
        return False

def fetch_combined_data(engine):
    df_original = pd.read_sql("SELECT * FROM patient_data", con=engine)
    df_new = pd.read_sql("SELECT * FROM predictions_log", con=engine)
    df_new = df_new[df_original.columns]  # Ensure same structure
    df_combined = pd.concat([df_original, df_new], ignore_index=True)
    return df_combined

def upload_data_to_s3(df, bucket, prefix):
    timestamp = datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")
    s3_key = f"{prefix}retraining_data_{timestamp}.csv"

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=s3_key, Body=csv_buffer.getvalue())
    print(f" Retraining CSV uploaded to s3://{bucket}/{s3_key}")
    return f"s3://{bucket}/{s3_key}"

def run_sagemaker_training(s3_input_uri):
    session = sagemaker.Session()
    xgb_image_uri = sagemaker.image_uris.retrieve("xgboost", REGION, version="1.5-1")

    estimator = Estimator(
        image_uri=xgb_image_uri,
        role=ROLE_ARN,
        instance_count=1,
        instance_type="ml.m5.large",
        output_path=ARTIFACT_PATH,
        sagemaker_session=session,
        base_job_name="xgb-retraining-job"
    )

    estimator.set_hyperparameters(
        objective="binary:logistic",
        num_round=100,
        max_depth=5,
        eta=0.2,
        subsample=0.8,
        colsample_bytree=0.8,
        eval_metric="logloss"
    )

    train_input = TrainingInput(
        s3_data=s3_input_uri,
        content_type="csv"
    )

    estimator.fit({"train": train_input})
    print(" SageMaker retraining job launched.")

def run_retraining_pipeline():
    engine = get_engine()

    if check_drift(engine):
        df = fetch_combined_data(engine)

        # Preprocess data: features + label only
        df = df[["age", "gender", "blood_pressure", "heart_rate", "cholesterol", "blood_sugar", "readmitted"]]
        df = pd.get_dummies(df, columns=["gender", "blood_pressure"], drop_first=True)

        # Save to S3
        s3_input_path = upload_data_to_s3(df, BUCKET, RETRAIN_PREFIX)

        # Launch training
        run_sagemaker_training(s3_input_path)
    else:
        print("ℹ Skipping retraining...")

if __name__ == "__main__":
    run_retraining_pipeline()
