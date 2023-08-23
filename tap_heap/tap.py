"""Heap tap class."""

from __future__ import annotations

from collections import defaultdict

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_heap import manifest, streams
from tap_heap.streams import HeapTableStream


class TapHeap(Tap):
    """Heap tap class."""

    name = "tap-heap"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "bucket",
            th.StringType,
            required=True,
            default="heap-rs3-yoga-is-dev",
            description="Heap connect bucket",
        ),
        th.Property(
            "additional_info",
            th.BooleanType,
            default=True,
            description="Add additional columns",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.HeapStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        streams = []

        manifests = manifest.generate_manifests(self.config["bucket"])
        sampled_manifest = {k: manifests[k] for k in list(manifests.keys())[:1]}
        table_name_to_columns = defaultdict(set)
        for all_table_manifests in sampled_manifest.values():
            for table_name, table_manifest in all_table_manifests.items():
                table_name_to_columns[table_name].update(set(table_manifest["columns"]))

        for table_name, columns in table_name_to_columns.items():
            stream = HeapTableStream(
                self, columns=columns, manifest_obj=manifests, name=table_name,
            )
            streams.append(stream)

        return streams


if __name__ == "__main__":
    TapHeap.cli()
