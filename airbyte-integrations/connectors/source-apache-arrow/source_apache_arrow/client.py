import json
import traceback
from typing import Iterable
from urllib.parse import urlparse

import pandas as pd
import pyarrow as pa
import smart_open
from airbyte_cdk.entrypoint import logger
from airbyte_cdk.models import AirbyteStream, SyncMode

from genson import SchemaBuilder


class ConfigurationError(Exception):
    """Client mis-configured"""


class PermissionsError(Exception):
    """User don't have enough permissions"""


class URLFile:

    def __init__(self, url: str, provider: dict):
        self._url = url
        self._provider = provider
        self._file = None

    def __enter__(self):
        return self._file

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


    def full_url(self):
        return f"{self.storage_scheme}{self.url}"

    def close(self):
        if self._file:
            self._file.close()
            self._file = None

    def open(self, binary=False):
        self.close()
        self._file = self._open(binary=binary)

        return self

    def _open(self, binary):
        mode = "rb" if binary else "r"
        storage = self.storage_scheme
        url = self.url

        return smart_open.open(self.full_url, mode=mode)

    @property
    def url(self) -> str:
        """Convert URL to remove the URL prefix (scheme)
        :return: the corresponding URL without URL prefix / scheme
        """
        parse_result = urlparse(self._url)
        if parse_result.scheme:
            return self._url.split("://")[-1]
        else:
            return self._url



    @property
    def storage_scheme(self) -> str:
        """Convert Storage Names to the proper URL Prefix
        :return: the corresponding URL prefix / scheme
        """
        storage_name = self._provider["storage"].upper()
        parse_result = urlparse(self._url)

        if storage_name == "LOCAL":
            return "file://"

        logger.error(f"Unknown Storage provider in: {self.full_url}")
        return ""


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

    @property
    def reader(self) -> URLFile:
        return self.reader_class(url=self._url, provider=self._provider)

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
        """load and return the appropriate pandas dataframe.

        :param fp: file-like object to read from
        :param skip_data: limit reading data
        :return: a list of dataframe loaded from files described in the configuration
        """

        readers = {
            # pandas.read_csv additional arguments can be passed to customize how to parse csv.
            # see https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html
            "csv": pd.read_csv,
            # We can add option to call to pd.normalize_json to normalize semi-structured JSON data into a flat table
            # by asking user to specify how to flatten the nested columns
            "flat_json": pd.read_json,
            "html": pd.read_html,
            "excel": pd.read_excel,
            "feather": pd.read_feather,
            "parquet": pd.read_parquet,
            "orc": pd.read_orc,
            "pickle": pd.read_pickle,
            "arrow": pa.ipc,
        }

        try:
            reader = readers[self._reader_format]
        except KeyError as err:
            error_msg = f"Reader {self._reader_format} is not supported\n{traceback.format_exc()}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg) from err

        reader_options = {**self._reader_options}
        if self._reader_format == "csv":
            reader_options["chunksize"] = 10000
            if skip_data:
                reader_options["nrows"] = 0
                reader_options["index_col"] = 0

            yield from reader(fp, **reader_options)
        else:
            yield reader(fp, **reader_options)

    @property
    def reader(self) -> URLFile:
        return self.reader_class(url=self._url, provider=self._provider)


    def get_binary_source(self):
        binary_formats = {"excel", "feather", "parquet", "orc", "pickle","arrow"}
        return self._reader_format in binary_formats

    def read(self, fields: Iterable = None) -> Iterable[dict]:
        """Read data from the stream"""
        if self._reader_format == "arrow":
            with pa.ipc.open_file(self._url) as fp:
                yield from fp.read_all()
        else:
            with self.reader.open(binary=self.binary_source) as fp:
                if self._reader_format == "json" or self._reader_format == "jsonl":
                    yield from self.load_nested_json(fp)
                else:
                    fields = set(fields) if fields else None
                    for df in self.load_dataframes(fp):
                        columns = fields.intersection(set(df.columns)) if fields else df.columns
                        df = df.where(pd.notnull(df), None)
                        yield from df[columns].to_dict(orient="records")

    def _stream_properties(self):
        with pa.ipc.open_file(self._url) as fp:
            print("TODO")

        with self.reader.open(binary=self.binary_source) as fp:
            if self._reader_format == "json" or self._reader_format == "jsonl":
                return self.load_nested_json_schema(fp)

            df_list = self.load_dataframes(fp, skip_data=False)
            fields = {}
            for df in df_list:
                for col in df.columns:
                    fields[col] = self.dtype_to_json_type(df[col].dtype)
            return {field: {"type": [fields[field], "null"]} for field in fields}

    def streams(self) -> Iterable:
        """Discovers available streams"""
        # TODO handle discovery of directories of multiple files instead
        json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {},
        }
        yield AirbyteStream(name=self.stream_name, json_schema=json_schema, supported_sync_modes=[SyncMode.full_refresh])
