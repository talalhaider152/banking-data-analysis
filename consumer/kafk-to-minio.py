import boto3
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

# -----------------------------
# Load secrets from .env
# -----------------------------
load_dotenv()

# Kafka consumer settings
try:
    consumer = KafkaConsumer(
        'banking_server.public.customers',
        'banking_server.public.accounts',
        'banking_server.public.transactions',
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=os.getenv("KAFKA_GROUP"),
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        request_timeout_ms=60000,
        session_timeout_ms=30000,
        api_version_auto_timeout_ms=10000,
        connections_max_idle_ms=600000,
        max_poll_records=100
    )
    print(f"✅ Connected to Kafka at {os.getenv('KAFKA_BOOTSTRAP')}")
except Exception as e:
    print(f"❌ Failed to connect to Kafka: {e}")
    print(f"   Bootstrap servers: {os.getenv('KAFKA_BOOTSTRAP')}")
    import time
    time.sleep(5)
    raise

# MinIO client
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
    verify=False
)

bucket = os.getenv("MINIO_BUCKET")


# Create bucket if not exists
if bucket not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
    s3.create_bucket(Bucket=bucket)

# Consume and write function
def write_to_minio(table_name, records):
    if not records:
        print("No records to write.")
        return
    print(f"Writing {len(records)} records to MinIO...")

    df = pd.DataFrame(records)
    date_str = datetime.now().strftime('%Y-%m-%d')
    
    # Use absolute path in temp directory
    temp_dir = os.path.join(os.getcwd(), 'temp')
    os.makedirs(temp_dir, exist_ok=True)
    file_path = os.path.join(temp_dir, f'{table_name}_{date_str}_{datetime.now().strftime("%H%M%S%f")}.parquet')
    
    print(f"Saving file to: {file_path}")
    try:
        # Convert all columns to strings to avoid encoding issues
        df = df.astype(str)
        df.to_parquet(file_path, engine='pyarrow', index=False)
        print(f"File saved successfully. File size: {os.path.getsize(file_path)} bytes")
    except Exception as e:
        print(f"Error saving parquet file: {e}")
        return

    s3_key = f'{table_name}/date={date_str}/{os.path.basename(file_path)}'
    print(f"Uploading file to s3://{bucket}/{s3_key}")
    try:
        s3.upload_file(file_path, bucket, s3_key)
        print(f'Uploaded {len(records)} records to s3://{bucket}/{s3_key}')
    except Exception as e:
        print(f"Error uploading file to MinIO: {e}")
    finally:
        # Clean up temp file
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"Cleaned up temp file: {file_path}")


# Batch consume
batch_size = 10
buffer = {
    'banking_server.public.customers': [],
    'banking_server.public.accounts': [],
    'banking_server.public.transactions': []
}

print("Connected to Kafka. Listening for messages...")

for message in consumer:
    print(message)
    topic = message.topic
    event = message.value
    payload = event.get("payload", {})
    record = payload.get("after")  # Only take the actual row
    print(record)

    if record:
        buffer[topic].append(record)
        print(f"[{topic}] -> {record}")  # Debugging
    else:
        print(f"[{topic}] No valid record found")

    if len(buffer[topic]) >= batch_size:
        write_to_minio(topic.split('.')[-1], buffer[topic])
        buffer[topic] = []