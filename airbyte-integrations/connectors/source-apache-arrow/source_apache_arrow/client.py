import json
import traceback
from typing import Iterable

import pandas as pd
import pyarrow as pa

from airbyte_cdk.entrypoint import logger
from airbyte_cdk.models import AirbyteStream, SyncMode

from genson import SchemaBuilder
import URLFile
import ConfigurationError
import PermissionsError


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
                raise ConfigurationError(error_msg) from err

    @property
    def stream_name(self) -> str:
        if self._dataset_name:
            return self._dataset_name
        return f"file_{self._provider['storage']}.{self._reader_format}"

    def load_nested_json_schema(self, fp) -> dict:
        # Use Genson Library to take JSON objects and generate schemas that describe them,
        builder = SchemaBuilder()
        if self._reader_format == "jsonl":
            for o in self.read():
                builder.add_object(o)
        else:
            builder.add_object(json.load(fp))

        result = builder.to_schema()
        if "items" in result and "properties" in result["items"]:
            result = result["items"]["properties"]
        return result 

    def load_nested_json(self, fp) -> list:
        if self._reader_format == "jsonl":
            result = []
            line = fp.readline()
            while line:
                result.append(json.loads(line))
                line = fp.readline()
        else:
            result = json.load(fp)
            if not isinstance(result, list):
                result = [result]
        return result

    def load_dataframes(self, fp, skip_data=False) -> Iterable:
        # load and return the appropriate pandas dataframe.

        # :param fp: file-like object to read from
        # :param skip_data: limit reading data
        # :return: a list of dataframe loaded from files described in the configuration

        readers = {
            # pandas.read_csv additional arguments can be passed to customize how to parse csv.
            # see https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html
            "csv": pd.read_csv,
            "arrow": pa.ipc,
        }

        try:
            reader = readers[self._reader_format]
        except KeyError as err:
            error_msg = f"Reader {self._reader_format} is not supported\n{traceback.format_exc()}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg) from err

        reader_options = {**self._reader_options}

        yield reader(fp, **reader_options)

    @property
    def reader(self) -> URLFile:
        return self.reader_class(url=self._url, provider=self._provider)

    @property
    def binary_source(self):
        binary_formats = {"excel", "feather", "parquet", "orc", "pickle", "arrow"}
        return self._reader_format in binary_formats

    def read(self, fields: Iterable = None) -> Iterable[dict]:
        """Read data from the stream"""
        if self._reader_format == "arrow":
            with pa.ipc.open_file(self._url) as fp:
                dataPandas = fp.read_pandas()

                yield from dataPandas[dataPandas.columns].to_dict(orient="records")
#               yield from fp.read_pandas()

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

    @property
    def streams(self) -> Iterable:
        """Discovers available streams"""
        # TODO handle discovery of directories of multiple files instead
        json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {},
        }
        yield AirbyteStream(name=self.stream_name, json_schema=json_schema, supported_sync_modes=[SyncMode.full_refresh])
