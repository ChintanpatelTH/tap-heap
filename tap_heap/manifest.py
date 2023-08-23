import json  # noqa: D100
import logging

from tap_heap import s3

logger = logging.getLogger(__name__)

def get_s3_manifest_file_contents(bucket: str)-> dict:
    """Get manifest contents."""
    manifests = s3.list_manifest_files_in_bucket(bucket)
    s3_client = s3.get_s3_client(bucket)
    # limit manifest files
    # manifests = list(manifests)[-5:]  # noqa: ERA001
    for manifest in manifests:
        contents = s3.get_file_handle(s3_client, manifest)
        yield json.loads(contents.read().decode("utf-8"))


def generate_manifests(bucket: str)-> dict:
    """Generate manifest object."""
    return {
        manifest["dump_id"]: {
            table["name"]: table
            for table in manifest["tables"]
            if table["name"] != "_event_metadata"
        }
        for manifest in get_s3_manifest_file_contents(bucket)
    }
