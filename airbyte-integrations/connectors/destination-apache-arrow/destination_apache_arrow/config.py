#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from __future__ import annotations

import json
import pyarrow as pa
import pyarrow.flight
from pyarrow._flight import FlightClient

from destination_apache_arrow.configuration_error import ConfigurationError


class DestinationApacheArrowConfig:
    DESTINATION_SERVER_FIELD = "destination_server"
    DESTINATION_PATH_FIELD = "destination_path"
    CHUNK_SIZE_FIELD = "chunk_size"

    _destination_path: str
    _destination_server: FlightClient
    _chunk_size: int

    @staticmethod
    def of(config: json) -> DestinationApacheArrowConfig:
        destination_server: FlightClient = DestinationApacheArrowConfig._get_destination_server(config)
        destination_path: str = DestinationApacheArrowConfig._get_destination_path(config)
        chunk_size: int = DestinationApacheArrowConfig._get_chunk_size(config)
        return DestinationApacheArrowConfig(destination_server, destination_path, chunk_size)

    def __init__(self, destination_server: FlightClient, destination_path: str, chunk_size: int):
        self._destination_server = destination_server
        self._chunk_size = chunk_size
        self._destination_path = destination_path

    @staticmethod
    def _get_destination_path(config: json) -> str:
        destination_path_option: str = config[DestinationApacheArrowConfig.DESTINATION_PATH_FIELD]
        if destination_path_option is None:
            raise ConfigurationError("Destination server parameter is missing.")
        return destination_path_option

    @staticmethod
    def _get_destination_server(config: json) -> FlightClient:
        destination_server_option: str = config[DestinationApacheArrowConfig.DESTINATION_SERVER_FIELD]
        if destination_server_option is None:
            raise ConfigurationError("Destination server parameter is missing.")
        return pa.flight.connect(destination_server_option)

    @staticmethod
    def _get_chunk_size(config: json) -> int:
        chunk_size = config[DestinationApacheArrowConfig.CHUNK_SIZE_FIELD]
        if chunk_size is None:
            raise ConfigurationError("Chunk size parameter is missing.")
        return chunk_size

    def get_chunk_size(self) -> int:
        return self._chunk_size

    def get_destination_server(self) -> FlightClient:
        return self._destination_server

    def get_destination_path(self) -> str:
        return self._destination_path
