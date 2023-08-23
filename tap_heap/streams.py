"""Stream type classes for tap-heap."""

from __future__ import annotations

import random
import typing as t
from pathlib import Path

import fastavro

from tap_heap import s3
from tap_heap.client import HeapStream
from tap_heap.utils import key_fn, remove_prefix


class HeapTableStream(HeapStream):
    """Define custom stream."""

    def get_rows(
        self, table_name: str, manifests: dict, context: dict
    ) -> t.Iterable[dict | tuple[dict, dict | None]]:
        """Download avro files and send records."""
        self.logger.info(self.starting_replication_key_value)
        table_manifests, should_create_new_version = self.filter_manifests_to_sync(
            manifests,
            table_name,
            self.starting_replication_key_value,
        )

        sync_files = self.get_files_to_sync(
            table_manifests,
            self.config["bucket"],
        )

        s3_client = s3.get_s3_client(self.config["bucket"])
        for sync_id, files in sync_files.items():
            for file in files:
                self.logger.info(f"Syncing file : {file}")  # noqa: G004
                line_number = 0
                local_file_suffix = random.randint(100000,999999)  # noqa: S311
                s3.download_file(s3_client, file, local_file_suffix)
                with Path(f"file_{local_file_suffix}.avro").open(mode="rb") as fo:
                    iterator = fastavro.reader(fo)
                    for row in iterator:
                        line_number += 1
                        yield self.add_additional_info(row=row, sync_id=sync_id)
                # Delete the file
                Path(f"file_{local_file_suffix}.avro").unlink()
            # Send state message after sync_id finish
            self._increment_stream_state(
                {self.replication_key: sync_id}, context=context,
            )
            self._write_state_message()

    def filter_manifests_to_sync(
        self,
        manifests: dict,
        table_name: str,
        replication_key: str,
    ) -> tuple[dict, bool]:
        """Filters a set of files for the table using 2 parts of the file name and drops
        up to the bookmark if there is a bookmark.
        """  # noqa: D205
        # table_manifest[dump_id] = {"files" ["file 1"], "incremental": True, "columns": ["column_1"]}  # noqa: E501
        table_manifests = {
            dump_id: manifest.get(table_name)
            for dump_id, manifest in manifests.items()
            if dump_id >= replication_key and manifest.get(table_name)
        }

        return (table_manifests, True)

    def get_files_to_sync(self, table_manifests: dict, bucket: str)-> dict:
        """Get flattened file names and remove the prefix."""
        files = {}
        for dump_id, manifest in table_manifests.items():
            files[dump_id] = sorted(
                [remove_prefix(file_name, bucket) for file_name in manifest["files"]],
                key=key_fn,
            )
        return files
