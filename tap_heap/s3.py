import logging  # noqa: D100
from pathlib import Path

import boto3

logger = logging.getLogger(__name__)


def list_manifest_files_in_bucket(bucket):  # noqa: ANN201, ANN001
    """Fetch manifest files."""
    s3_paginator = boto3.client("s3").get_paginator("list_objects_v2")

    s3_objects = {}
    max_results = 1000
    args = {
        "Bucket": bucket,
        "MaxKeys": max_results,
    }

    args["Prefix"] = "manifests"
    page_iterator = s3_paginator.paginate(**args)

    for response in page_iterator:
        for object_data in response["Contents"]:
            key = object_data["Key"]
            if key.endswith(".json"):
                s3_objects[object_data["Key"]] = object_data["LastModified"]

    sorted(s3_objects, key=lambda k: k[1])
    return s3_objects.keys()


def get_file_handle(client, s3_path):  # noqa: ANN201, D103, ANN001
    s3_object = client.Object(s3_path)
    return s3_object.get()["Body"]


def download_file(client, s3_path: str, suffix: int):
    """Download file to local disk."""
    with Path(f"file_{suffix}.avro").open(mode="wb") as f:
        client.download_fileobj(s3_path, Fileobj=f)


def get_s3_resource(bucket: str):
    """Get S3 resource."""
    s3 = boto3.resource("s3")
    return s3.Bucket(bucket)

def get_s3_client():
    """Get S3 client."""
    return boto3.client("s3")

def get_modified_date(client, bucket: str, key: str):
    response = client.head_object(
        Bucket=bucket,
        Key=key,
    )
    return response["LastModified"]
