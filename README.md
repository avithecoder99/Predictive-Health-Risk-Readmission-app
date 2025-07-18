# Real-Time Healthcare AI System (SQL + S3 + Kinesis)

Simulates a real hospital pipeline:
- Extracts patient data (demographics/labs) from a SQL database
- Streams vitals data using Kinesis-like logic
- Trains an XGBoost model using S3-stored training data
- Performs real-time predictions via FastAPI
- Logs all inference input + output to S3
- Stores results back into SQL DB for retraining

### Tech Stack:
FastAPI • SQLAlchemy • boto3 • XGBoost • Pandas • Kinesis (simulated) • S3
