import json
import traceback
from typing import Iterable

import pandas as pd
import pyarrow as pa
import numpy as np
import time


from airbyte_cdk.entrypoint import logger
from airbyte_cdk.models import AirbyteStream, SyncMode
from genson import SchemaBuilder
from source_apache_arrow.URLFile import URLFile


class Client:
    """Class that manages reading and parsing data from streams"""

    reader_class = URLFile

    def __init__(self, dataset_name: str, url: str, provider: dict, format: str = None, reader_options: str = None):
        self._dataset_name = dataset_name
        self._url = url
        self._provider = provider
        self._reader_format = format or "csv"
        self._reader_options = {}
        if reader_options:
            try:
                self._reader_options = json.loads(reader_options)
            except json.decoder.JSONDecodeError as err:
                error_msg = f"Failed to parse reader options {repr(err)}\n{reader_options}\n{traceback.format_exc()}"
                logger.error(error_msg)
                raise Exception(error_msg) from err

    @property
    def stream_name(self) -> str:
        if self._dataset_name:
            return self._dataset_name
        return f"file_{self._provider['storage']}.{self._reader_format}"

    @property
    def reader(self) -> URLFile:
        return self.reader_class(url=self._url, provider=self._provider)

    def read(self, fields: Iterable = None):
        """Read data from the stream"""
        if self._reader_format == "arrow":
            open_file_start=time.perf_counter()
            with pa.ipc.open_file(self._url) as fp:
                open_file_stop = time.perf_counter()
                dataPandas = fp.read_pandas()
                fields = set(fields) if fields else None
                columns = fields.intersection(set(dataPandas.columns)) if fields else dataPandas.columns
                yield from dataPandas[columns].to_dict(orient="records")
            logger.info("timer for open: " + str(open_file_stop - open_file_start))

    @staticmethod
    def dtype_to_json_type(dtype) -> str:
        """Convert Pandas Dataframe types to Airbyte Types.

        :param dtype: Pandas Dataframe type
        :return: Corresponding Airbyte Type
        """
        if dtype == object:
            return "string"
        elif dtype in ("int64", "float64"):
            return "number"
        elif dtype == "bool":
            return "boolean"
        return "string"

    def _stream_properties(self):
        with pa.ipc.open_file(self._url) as fp:
            df = fp.read_pandas()
            fields = {}
            for col in df.columns:  # works if there is only one DATAFRAME, otherwise we need to add another for loop
                fields[col] = self.dtype_to_json_type(df[col].dtype)
            return {field: {"type": [fields[field], "null"]} for field in fields}

    @property
    def streams(self) -> Iterable:
        """Discovers available streams"""
        # TODO handle discovery of directories of multiple files instead
        json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": self._stream_properties(),
        }
        yield AirbyteStream(name=self.stream_name, json_schema=json_schema, supported_sync_modes=[SyncMode.full_refresh])
