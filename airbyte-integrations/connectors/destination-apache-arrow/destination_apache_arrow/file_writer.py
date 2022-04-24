#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

import json

from airbyte_cdk import logger
from airbyte_cdk.entrypoint import logger
from airbyte_cdk.models import AirbyteRecordMessage, ConfiguredAirbyteStream
from build.lib.destination_apache_arrow.config import DestinationApacheArrowConfig
from pandas import DataFrame
from pyarrow import DataType, FileOutputStream, RecordBatch, RecordBatchStreamWriter, Schema, Table, bool_, float64, fs, schema, utf8


class DestinationApacheArrowFileWriter:

    total_size: float
    chunk_size: int
    chunk_index: int
    schema_root: Schema
    field_type_map: dict[str, str]
    file_writer: RecordBatchStreamWriter
    output_stream: FileOutputStream
    batch: DataFrame

    def __init__(self, config: DestinationApacheArrowConfig, configured_stream: ConfiguredAirbyteStream):
        self.chunk_size = config.get_chunk_size()
        self.total_size = 0
        self.chunk_index = 0

        self.field_type_map = self._get_field_type_map(configured_stream.stream.json_schema)
        self.schema_root = self._get_schema(self.field_type_map)

        local = fs.LocalFileSystem()
        file_path = f"{config.get_destination_path()}/{configured_stream.stream.name}.arrow"
        self.output_stream = local.open_output_stream(file_path)
        self.file_writer = RecordBatchStreamWriter(self.output_stream, self.schema_root)

        self.batch = self._get_dataframe()

    def write(self, record_message: AirbyteRecordMessage):
        logger.info("Start writing")
        self.batch.append(self._extract_data(record_message.data))
        self.chunk_index += 1

        if self.chunk_index == self.chunk_size:
            self._save_chunk()

    def flush(self):
        return self._save_chunk()

    def close(self):
        self._save_chunk()
        self.file_writer.close()
        self.output_stream.close()

    @staticmethod
    def _get_field_type_map(schema: json) -> dict[str, str]:
        fields = schema["properties"]
        return {key: value for key, value in fields.items()}

    @staticmethod
    def _get_schema(field_type_map: dict[str, str]) -> Schema:
        return schema([(field, DestinationApacheArrowFileWriter._get_field_type(airbyte_type))
                       for field, airbyte_type in field_type_map.items()])

    @staticmethod
    def _extract_data(data: json) -> list:
        value_list = []
        for (key, value) in data:
            value_list.append(value)
        return value_list

    @staticmethod
    def _get_field_type(data_type: str) -> DataType:
        match data_type:
            case 'boolean':
                return bool_()
            case 'number':
                return float64()
            case 'array':
                return utf8()
            case 'object':
                return utf8()
            case 'null':
                return utf8()
            case 'string':
                return utf8()
            case _:
                return utf8()

    @staticmethod
    def _get_dataframe_type(data_type: str) -> str:
        match data_type:
            case 'boolean':
                return 'bool'
            case 'number':
                return 'float'
            case 'array':
                return 'str'
            case 'object':
                return 'str'
            case 'null':
                return 'str'
            case 'string':
                return 'str'
            case _:
                return 'str'

    def _save_chunk(self):
        self.total_size += self.chunk_index
        logger.info(f'Filled chunk with {self.chunk_index} items; {self.total_size} items written')
        logger.info(f'Chunk written')
        self.file_writer.write_table(Table.from_pandas(self.batch, preserve_index=False, schema=self.schema_root))
        self.batch = self.batch[0:0]
        self.chunk_index = 0

    def _get_dataframe(self) -> DataFrame:
        return DataFrame({column_name: DestinationApacheArrowFileWriter._get_dataframe_type(type) for (column_name, type) in self.field_type_map})


