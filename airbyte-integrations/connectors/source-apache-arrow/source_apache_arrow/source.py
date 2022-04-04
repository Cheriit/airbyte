#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#
import os


import traceback
import json
from datetime import datetime
from typing import Generator, Iterable, Mapping
import pyarrow as pa

from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    Status,
    Type,
)
from airbyte_cdk.sources import Source

from source_apache_arrow.client import Client


class SourceApacheArrow(Source):
    client_class = Client

    def _get_client(self, config: Mapping):
        """Construct client"""
        client = self.client_class(**config)

        return client

    def check(self, logger: AirbyteLogger, config: json) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the integration
            e.g: if a provided Stripe API token can be used to connect to the Stripe API.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
        the properties of the spec.json file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        client = self._get_client(config)
        os.system("dir")
        try:
            with pa.ipc.open_file(client.reader.url):
                return AirbyteConnectionStatus(status=Status.SUCCEEDED);

        except Exception as err:
            reason = f"Failed to load: {repr(err)}\n{traceback.format_exc()}"
            logger.error(reason)
            return AirbyteConnectionStatus(status=Status.FAILED, message=reason)

    def discover(self, logger: AirbyteLogger, config: json) -> AirbyteCatalog:
        """
        Returns an AirbyteCatalog representing the available streams and fields in this integration.
        For example, given valid credentials to a Postgres database,
        returns an Airbyte catalog where each postgres table is a stream, and each table column is a field.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
        the properties of the spec.json file

        :return: AirbyteCatalog is an object describing a list of all available streams in this source.
            A stream is an AirbyteStream object that includes:
            - its stream name (or table name in the case of Postgres)
            - json_schema providing the specifications of expected schema for this stream (a list of columns described
            by their names and types)
        """
        client = self._get_client(config)
        name = client.stream_name

        logger.info(f"Discovering schema of {name} at {client.reader.full_url}...")
        try:
            streams = list(client.streams)
        except Exception as err:
            reason = f"Failed to discover schemas of {name} at {client.reader.full_url}: {repr(err)}\n{traceback.format_exc()}"
            logger.error(reason)
            raise err
        return AirbyteCatalog(streams=streams)

    def read(
            self, logger: AirbyteLogger, config: Mapping, catalog: ConfiguredAirbyteCatalog, state_path: Mapping[str, any]
    ) -> Generator[AirbyteMessage, None, None]:
        """Returns a generator of the AirbyteMessages generated by reading the source with the given configuration, catalog, and state."""
        client = self._get_client(config)
        fields = self.selected_fields(catalog)
        name = client.stream_name

        logger.info(f"Reading {name} ({client.reader.full_url})...")
        try:
            for row in client.read(fields=fields):
                new_data = {}
                count=0;
                for item in row:
                    count+=1
                    new_data["odczytana wartosc " +str(count) ] = str(item)

                record = AirbyteRecordMessage(stream=name, data=new_data, emitted_at=int(datetime.now().timestamp()) * 1000)
                yield AirbyteMessage(type=Type.RECORD, record=record)
        except Exception as err:
            reason = f"Failed to read data of {name} at {client.reader.full_url}: {repr(err)}\n{traceback.format_exc()}"
            logger.error(reason)
            raise err

    @staticmethod
    def selected_fields(catalog: ConfiguredAirbyteCatalog) -> Iterable:
        for configured_stream in catalog.streams:
            yield from configured_stream.stream.json_schema["properties"].keys()
