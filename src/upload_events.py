import os
import re

import boto3
from botocore.client import Config

# The endpoint this process talks to SeaweedFS on. `S3_PUBLIC_ENDPOINT_URL` is the one a
# browser reaches it on: a presigned URL is signed against the host the client will use,
# which is not always the host the server uses.
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://127.0.0.1:8333")
S3_PUBLIC_ENDPOINT_URL = os.getenv("S3_PUBLIC_ENDPOINT_URL", S3_ENDPOINT_URL)
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID", "smart_files")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY", "smart_files_secret")
S3_BUCKET = os.getenv("S3_BUCKET", "smart-files")
ARTIFACT_PREFIX = os.getenv("ARTIFACT_PREFIX", "ingested").strip("/")


def _client(endpoint_url: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def s3_client():
    """A client for this process's own reads and writes."""
    return _client(S3_ENDPOINT_URL)


def presign_client():
    """A client whose signatures are valid for the endpoint the client will call."""
    return _client(S3_PUBLIC_ENDPOINT_URL)


def safe_filename(filename: str) -> str:
    basename = filename.replace("\\", "/").rsplit("/", 1)[-1].strip()
    cleaned = re.sub(r"[^A-Za-z0-9._-]", "_", basename)
    return cleaned if cleaned not in {"", ".", ".."} else "upload.bin"
