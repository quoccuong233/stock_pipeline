import os
import shutil
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Credentials come from .env, passed into the Airflow containers via docker-compose env_file
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
BUCKET = "bronze-transactions"
LOCAL_DIR = "/tmp/minio_downloads"  # use absolute path for Airflow

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DATABASE", "STOCK_MDS")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "COMMON")

PROCESSED_PREFIX = "da_xu_ly/"  # files already loaded into Snowflake are moved here

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

def download_from_minio():
    # Start from an empty dir so files from previous runs are not uploaded again
    if os.path.isdir(LOCAL_DIR):
        shutil.rmtree(LOCAL_DIR)
    os.makedirs(LOCAL_DIR)
    s3 = get_s3_client()

    # list_objects_v2 returns at most 1000 keys per call, so paginate
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET):
        for obj in page.get("Contents", []):
            if not obj["Key"].startswith(PROCESSED_PREFIX):
                keys.append(obj["Key"])

    for key in keys:
        # Keep the symbol in the file name: "AAPL/123.json" -> "AAPL_123.json"
        local_file = os.path.join(LOCAL_DIR, key.replace("/", "_"))
        s3.download_file(BUCKET, key, local_file)
    print(f"Downloaded {len(keys)} files from MinIO")
    return keys

def load_to_snowflake(**kwargs):
    keys = kwargs['ti'].xcom_pull(task_ids='download_minio')
    if not keys:
        print("No files to load.")
        return

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA
    )
    cur = conn.cursor()

    cur.execute(f"PUT file://{LOCAL_DIR}/*.json @%bao_gia_co_phieu_tho OVERWRITE = TRUE")
    print(f"Uploaded {len(keys)} files to Snowflake stage")

    # PURGE removes staged files after loading so the stage does not keep growing
    cur.execute("""
        COPY INTO bao_gia_co_phieu_tho
        FROM @%bao_gia_co_phieu_tho
        FILE_FORMAT = (TYPE=JSON)
        PURGE = TRUE
    """)
    print("COPY INTO executed")

    cur.close()
    conn.close()

def archive_in_minio(**kwargs):
    keys = kwargs['ti'].xcom_pull(task_ids='download_minio')
    if not keys:
        return
    s3 = get_s3_client()
    for key in keys:
        s3.copy_object(Bucket=BUCKET, Key=PROCESSED_PREFIX + key,
                       CopySource={"Bucket": BUCKET, "Key": key})
        s3.delete_object(Bucket=BUCKET, Key=key)
    print(f"Moved {len(keys)} files to {PROCESSED_PREFIX}")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 10, 10),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "minio_to_snowflake",
    default_args=default_args,
    schedule_interval="*/1 * * * *",  # every 1 minutes
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    task2 = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
        provide_context=True,
    )

    task3 = PythonOperator(
        task_id="archive_minio",
        python_callable=archive_in_minio,
    )

    task1 >> task2 >> task3
