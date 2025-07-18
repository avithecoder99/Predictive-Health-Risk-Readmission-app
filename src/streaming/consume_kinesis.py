import boto3
import json
import requests
import time
import pandas as pd
from sqlalchemy import create_engine

# AWS Kinesis Config
STREAM_NAME = "patient_vitals_stream"
REGION = "us-east-1"
PREDICTION_API_URL = "http://127.0.0.1:8000/predict"

# SQL Config
def get_sql_engine():
    return create_engine("mysql+pymysql://admin:admin123@localhost:3306/hospital_db")

def fetch_sql_data(patient_id):
    engine = get_sql_engine()
    query = f"SELECT * FROM patient_data WHERE patient_id = '{patient_id}'"
    df = pd.read_sql(query, engine)
    return df.iloc[0].to_dict() if not df.empty else None

def process_record(vitals):
    patient_id = vitals["patient_id"]
    demographics = fetch_sql_data(patient_id)
    if not demographics:
        print(f"❌ Patient {patient_id} not found in SQL.")
        return

    input_data = {
        "patient_id": patient_id,
        "age": demographics["age"],
        "gender": demographics["gender"],
        "blood_pressure": vitals["blood_pressure"],
        "heart_rate": vitals["heart_rate"],
        "cholesterol": demographics["cholesterol"],
        "blood_sugar": demographics["blood_sugar"],
        "oxygen_saturation": vitals["oxygen_saturation"],
        "temperature": vitals["temperature"]
    }

    try:
        response = requests.post(PREDICTION_API_URL, json=input_data)
        print(f" Prediction for {patient_id}: {response.json()}")
    except Exception as e:
        print(f" Error calling prediction API: {e}")

def consume_kinesis():
    client = boto3.client("kinesis", region_name=REGION)
    shard_id = "shardId-000000000000"
    response = client.get_shard_iterator(
        StreamName=STREAM_NAME,
        ShardId=shard_id,
        ShardIteratorType="LATEST"
    )
    shard_iterator = response["ShardIterator"]

    while True:
        records_response = client.get_records(ShardIterator=shard_iterator, Limit=10)
        records = records_response["Records"]
        for record in records:
            vitals = json.loads(record["Data"])
            process_record(vitals)
        shard_iterator = records_response["NextShardIterator"]
        time.sleep(5)

if __name__ == "__main__":
    consume_kinesis()
