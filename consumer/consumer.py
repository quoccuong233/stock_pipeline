#Import requirements
import os
import json
import boto3
import time
from dotenv import load_dotenv
from kafka import KafkaConsumer

#Minio Connection (credentials are read from .env)
load_dotenv()
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9002",
    aws_access_key_id=os.environ["MINIO_ROOT_USER"],
    aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"]
)

bucket_name = "bronze-transactions"

# Ensure bucket exists (idempotent)
try:
    s3.head_bucket(Bucket=bucket_name)
    print(f"Bucket {bucket_name} already exists.")
except Exception:
    s3.create_bucket(Bucket=bucket_name)
    print(f"Created bucket {bucket_name}.")

#Define Consumer
consumer = KafkaConsumer(
    "stock-quotes",
    bootstrap_servers=["localhost:29092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="bronze-consumer1",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("Consumerstreaming and saving to MinIO...")

#Main Function
for message in consumer:
    record = message.value
    symbol = record.get("symbol", "unknown")
    ts = record.get("fetched_at",int(time.time()))
    key = f"{symbol}/{ts}.json"

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(record),
        ContentType="application/json"
    )
    print(f"Saved record for {symbol} = s3://{bucket_name}/{key}")
                    