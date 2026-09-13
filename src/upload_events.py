import os
import re

import boto3
from botocore.client import Config


S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://127.0.0.1:8333")
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID", "smart_files")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY", "smart_files_secret")
S3_BUCKET = os.getenv("S3_BUCKET", "smart-files")
RABBITMQ_URL = os.getenv(
    "RABBITMQ_URL", "amqp://smart_files:smart_files@127.0.0.1:5673/%2F"
)
UPLOAD_EXCHANGE = os.getenv("UPLOAD_EXCHANGE", "file-uploads")
INGEST_QUEUE = os.getenv("INGEST_QUEUE", "file-ingestion")
COMPLETION_EXCHANGE = os.getenv("COMPLETION_EXCHANGE", "ingestion-results")
PROCESS_QUEUE = os.getenv("PROCESS_QUEUE", "file-processing")
ARTIFACT_PREFIX = os.getenv("ARTIFACT_PREFIX", "ingested").strip("/")


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def safe_filename(filename: str) -> str:
    basename = filename.replace("\\", "/").rsplit("/", 1)[-1].strip()
    cleaned = re.sub(r"[^A-Za-z0-9._-]", "_", basename)
    return cleaned if cleaned not in {"", ".", ".."} else "upload.bin"
