import boto3
import json
import random
import time
from datetime import datetime

# AWS Kinesis Config
STREAM_NAME = "patient_vitals_stream"
REGION = "us-east-1"

# Initialize Kinesis client
kinesis = boto3.client("kinesis", region_name=REGION)

# Simulated patient vitals
def generate_vital_data():
    patient_id = f"P{random.randint(100, 999)}"
    return {
        "patient_id": patient_id,
        "timestamp": datetime.utcnow().isoformat(),
        "heart_rate": random.randint(60, 130),
        "blood_pressure": random.choice(["120/80", "130/85", "140/90"]),
        "oxygen_saturation": round(random.uniform(90, 100), 2),
        "temperature": round(random.uniform(36.0, 39.0), 1)
    }

def stream_to_kinesis(interval=5):
    print(f" Streaming to Kinesis stream: {STREAM_NAME}")
    while True:
        data = generate_vital_data()
        kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(data),
            PartitionKey=data["patient_id"]
        )
        print(f" Sent to Kinesis: {data}")
        time.sleep(interval)

if __name__ == "__main__":
    stream_to_kinesis()
