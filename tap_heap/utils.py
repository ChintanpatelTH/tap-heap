import re


def remove_prefix(file_name, bucket):
    path_prefix = f"s3://{bucket}/"
    return file_name.replace(path_prefix, "")

def key_fn(key: str):  # noqa: ANN201
    """This function ensures we sort a list of manifest files based on the 'sync_id' and 'part_id'
    For example given a key of:
    'sync_852/sessions/part-00000-4a06bab5-0ef3-4b21-b9af-e772fbb37b0e-c000.avro'
    This function returns a tuple: (int("852"), int("00000").
    """  # noqa: D205
    file_path = key.split("/")
    dump_id = file_path[0].replace("sync_", "")
    part_number = re.findall("([0-9]+)", file_path[-1])[0]
    return (int(dump_id), int(part_number))
