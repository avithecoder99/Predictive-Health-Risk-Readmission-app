# src/sql_to_s3/extract_from_sql.py

import pandas as pd
import boto3
import os
from datetime import datetime
from src.sql.connect_sql import get_sql_connection

# -------- Configuration --------
TABLE_NAME = "patient_data"
S3_BUCKET = "your-s3-bucket-name"
S3_KEY_PREFIX = "training_data/"
LOCAL_CSV_PATH = "sample_data/patient_records/patient_data_export.csv"

# -------- SQL Query Template --------
SQL_QUERY = f"""
SELECT
    patient_id,
    age,
    gender,
    blood_pressure,
    heart_rate,
    cholesterol,
    blood_sugar,
    diagnosis,
    readmitted
FROM {TABLE_NAME};
"""

def upload_to_s3(local_file, bucket, s3_key):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(local_file, bucket, s3_key)
        print(f" Uploaded to S3: s3://{bucket}/{s3_key}")
    except Exception as e:
        print(f" Failed to upload to S3: {e}")

def extract_and_upload():
    print(" Connecting to SQL database...")
    engine, _ = get_sql_connection(
        db_type="mysql",       # or "postgresql"
        username="admin",
        password="admin123",
        host="localhost",
        port="3306",
        database="hospital_db"
    )

    if not engine:
        print(" SQL connection failed. Aborting.")
        return

    print(" Pulling data from SQL...")
    try:
        df = pd.read_sql(SQL_QUERY, con=engine)
        print(f" Retrieved {len(df)} records.")
    except Exception as e:
        print(f" Failed to query SQL: {e}")
        return

    print(" Saving locally as CSV...")
    os.makedirs(os.path.dirname(LOCAL_CSV_PATH), exist_ok=True)
    df.to_csv(LOCAL_CSV_PATH, index=False)

    print(" Uploading CSV to S3...")
    timestamp = datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")
    s3_key = f"{S3_KEY_PREFIX}patient_data_{timestamp}.csv"
    upload_to_s3(LOCAL_CSV_PATH, S3_BUCKET, s3_key)

if __name__ == "__main__":
    extract_and_upload()
