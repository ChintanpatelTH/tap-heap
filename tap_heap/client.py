"""Custom client handling, including HeapStream base class."""

from __future__ import annotations

from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable

from singer_sdk.streams import Stream

if TYPE_CHECKING:
    from os import PathLike

    import singer_sdk._singerlib as singer
    from singer_sdk.tap_base import Tap

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class HeapStream(Stream):
    """Stream class for Heap streams."""

    def __init__(
        self,
        tap: Tap,
        schema: str | PathLike | dict[str, Any] | singer.Schema | None = None,
        name: str | None = None,
        columns: list[str] = [],
        manifest_obj={},
    ) -> None:
        """Duplicates superclass functionality but runs replication config before init.

        Raises:
            RuntimeError: If replication config is invalid.
        """
        # Define starting_replication_key_value based on state, stream_name, and
        # start_date. This has to be done before stream initialization below so that
        # state can be used during the discovery process.
        self.schema_cols = columns
        self.manifest = manifest_obj
        self.starting_replication_key_value: str | None = None
        if tap.state:
            stream_name = name
            if stream_name not in tap.state["bookmarks"]:
                msg = (
                    "State was passed so incremental replication is assumed. However, "
                    f"no state was found for a stream_name of {stream_name}."
                )
                self.starting_replication_key_value = 0
            else:
                self.starting_replication_key_value = tap.state["bookmarks"][
                    stream_name
                ]["replication_key_value"]
        else:
            self.starting_replication_key_value = 0

        super().__init__(tap, schema, name)

        # If _sdc_last_modified is not in the stream, incremental replication cannot
        # be used.
        if not (
            self.starting_replication_key_value is None
            or self.config["additional_info"]
        ):
            msg = "Incremental replication requires additional_info to be True."
            raise RuntimeError(msg)

        # This is set to a constant because the tap only supports _sdc_last_modified as
        # an incremental replication key, not custom values.
        self.replication_key = "_sdc_sync_id"

    @property
    def primary_keys(self) -> list[str] | None:
        """Get primary keys.

        Returns:
            A list of primary key(s) for the stream.
        """
        return self.get_key_properties(self.name)

    @primary_keys.setter
    def primary_keys(self, new_value: list[str] | None) -> None:
        """Set primary key(s) for the stream.

        Args:
            new_value: TODO
        """
        self._primary_keys = new_value

    @property
    def schema_filepath(self) -> Path | None:
        """Get path to schema file.

        Returns:
            Path to a schema file for the stream or `None` if n/a.
        """
        return SCHEMAS_DIR / Path(f"{self.name}.json")

    def get_records(
        self,
        context: dict | None,
    ) -> Iterable[dict]:
        """Return a generator of record-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.

        Args:
            context: Stream partition or context dictionary.

        Raises:
            NotImplementedError: If the implementation is TODO
        """
        yield from self.get_rows(
            table_name=self.name,
            manifests=self.manifest,
            context=context,
        )

    @property
    def is_sorted(self) -> bool:
        """The stream returns records in order."""
        return True

    @property
    def check_sorted(self) -> bool:
        """Check if stream is sorted.

        This setting enables additional checks which may trigger
        `InvalidStreamSortException` if records are found which are unsorted.

        Returns:
            `True` if sorting is checked. Defaults to `True`.
        """
        return False

    # @cached_property
    # def schema(self) -> dict:
    #     """Orchestrates schema creation for all streams.

    #     Returns:
    #         A schema constructed using the get_properties() method of whichever stream
    #         is currently in use.
    #     """
    #     properties = self.get_properties()
    #     additional_info = self.config["additional_info"]
    #     if additional_info:
    #         properties.update({"_sdc_sync_id": {"type": "string"}})
    #     return {"properties": properties}

    # def get_properties(self) -> dict:
    #     """Get a list of properties for a *SV file, to be used in creating a schema.

    #     Each column in the *SV will have its own entry in the schema. All entries will
    #     be of the form: `'FIELD_NAME': {'type': ['null', 'string']}`

    #     Returns:
    #         A list of properties representing a *SV file.
    #     """
    #     properties = {}

    #     for field in self.schema_cols:
    #         properties.update({field: {"type": ["null", "string"]}})

    #     return properties

    def get_key_properties(self, table_name: str) -> list:
        """Get key properties based on table name."""
        if table_name == "user_migrations":
            kp = ["from_user_id"]
        elif table_name == "users":
            kp = ["user_id"]
        else:
            kp = ["event_id"]
        return kp

    def add_additional_info(
        self,
        row: dict,
        sync_id: str,
    ) -> dict:
        """Adds _sdc-prefixed additional columns to a row, dependent on config.

        Args:
            row: The row to add info to.
            file_name: The name of the file that the row came from.
            line_number: The line number of the row within its file.
            last_modified: The last_modified date of the row's file.

        Returns:
            A dictionary representing a row containing additional information columns.
        """
        additional_info = self.config["additional_info"]
        if additional_info:
            row.update({"_sdc_sync_id": sync_id})
        return row
