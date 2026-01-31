FROM python:3.12-slim

WORKDIR /app

# Install Python dependencies
RUN pip install --no-cache-dir \
    kafka-python \
    boto3 \
    pandas \
    python-dotenv \
    fastparquet \
    pyarrow

# Copy the consumer script
COPY kafk-to-minio.py .
COPY .env .

# Run the consumer
CMD ["python", "kafk-to-minio.py"]
