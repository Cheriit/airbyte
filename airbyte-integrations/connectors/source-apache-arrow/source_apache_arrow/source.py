#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#
import os
import traceback
import json
from datetime import datetime
from typing import Generator, Iterable, Mapping
import pyarrow as pa
import pyarrow.flight
import time

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
        ip_address = config.get("ipAddress")
        os.system("dir")
        try:
            clientArrowFlight = pa.flight.connect("grpc://" + ip_address + ":8815")
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)

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
        try:
            streams = list(client.streams)
        except Exception as err:
            reason = f"Failed to discover schemas of {name}: {repr(err)}\n{traceback.format_exc()}"
            logger.error(reason)
            raise err
        return AirbyteCatalog(streams=streams)

    def read(
            self, logger: AirbyteLogger, config: Mapping, catalog: ConfiguredAirbyteCatalog, state_path: Mapping[str, any]
    ) -> Generator[AirbyteMessage, None, None]:
        """Returns a generator of the AirbyteMessages generated by reading the source with the given configuration, catalog, and state."""
        timer_start = time.perf_counter()
        client = self._get_client(config)
        fields = self.selected_fields(catalog)
        name = client.stream_name
        # logger.info(f"Reading {name} ({client.reader.full_url})...")
        try:
            #    for i in fields:
            #         logger.info("field source: " + i)
            #  clientArrowFlight = pa.flight.connect("grpc://172.17.0.2:8815")

            # upload_descriptor = pa.flight.FlightDescriptor.for_path("uploaded.parquet")
            '''
            flight = clientArrowFlight.get_flight_info(upload_descriptor)
            descriptor = flight.descriptor
            print("Path:", descriptor.path[0].decode('utf-8'), "Rows:", flight.total_records, "Size:", flight.total_bytes)
            print("=== Schema ===")
            print(flight.schema)
            print("==============")

            reader = clientArrowFlight.do_get(flight.endpoints[0].ticket)
            read_table = reader.read_all()
            print(read_table.to_pandas().head())'''
            data_pandas = client.read(fields)
            count = 0
            for j in data_pandas:
                new_data = {"rekord " + str(count): j}
                count += 1
                record = AirbyteRecordMessage(stream=name, data=new_data, emitted_at=int(datetime.now().timestamp()) * 1000)
                yield AirbyteMessage(type=Type.RECORD, record=record)
        except Exception as err:
            #  reason = f"Failed to read data of {name} at {client.reader.full_url}: {repr(err)}\n{traceback.format_exc()}"
            #  logger.error(reason)
            raise err
        timer_stop = time.perf_counter()
        logger.info("timer: " + str(timer_stop - timer_start))

        '''data_pandas = client.read(fields)
            new_data = {}
            count = 0
            logger.info("for fata pandas")
            for j in data_pandas:
                new_data["rekord " + str(count)] = j
                count += 1
                if count % 1000 == 0:
                    logger.info("nr " + str(count))
                record = AirbyteRecordMessage(stream=name, data=new_data, emitted_at=int(datetime.now().timestamp()) * 1000)
                yield AirbyteMessage(type=Type.RECORD, record=record)
        except Exception as err:
            reason = f"Failed to read data of {name} at {client.reader.full_url}: {repr(err)}\n{traceback.format_exc()}"
            logger.error(reason)
            raise err
        timer_stop = time.perf_counter()
        logger.info("timer: " + str(timer_stop - timer_start))
        '''

    @staticmethod
    def selected_fields(catalog: ConfiguredAirbyteCatalog) -> Iterable:
        for configured_stream in catalog.streams:
            yield from configured_stream.stream.json_schema["properties"].keys()
