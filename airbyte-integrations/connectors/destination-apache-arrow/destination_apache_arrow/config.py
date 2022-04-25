#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from __future__ import annotations

import json
from pathlib import Path
from destination_apache_arrow.configuration_error import ConfigurationError


class DestinationApacheArrowConfig:
    DESTINATION_PATH_FIELD = "destination_path"
    CHUNK_SIZE_FIELD = "chunk_size"

    _destination_path: Path
    _chunk_size: int

    @staticmethod
    def of(config: json) -> DestinationApacheArrowConfig:
        destination_path: Path = DestinationApacheArrowConfig._get_destination_path(config)
        chunk_size: int = DestinationApacheArrowConfig._get_chunk_size(config)
        return DestinationApacheArrowConfig(destination_path, chunk_size)

    def __init__(self, destination_path: Path, chunk_size: int):
        self._destination_path = destination_path
        self._chunk_size = chunk_size

    @staticmethod
    def _get_destination_path(config: json) -> Path:
        destination_path_option: str = config[DestinationApacheArrowConfig.DESTINATION_PATH_FIELD]
        if destination_path_option is None:
            raise ConfigurationError("Destination path parameter is missing.")
        if not destination_path_option.startswith("/local"):
            destination_path_option = f"$/local/{destination_path_option}"
        destination_path: Path = Path(destination_path_option)
        return destination_path

    @staticmethod
    def _get_chunk_size(config: json) -> int:
        chunk_size = config[DestinationApacheArrowConfig.CHUNK_SIZE_FIELD]
        if chunk_size is None:
            raise ConfigurationError("Chunk size parameter is missing.")
        return chunk_size

    def get_chunk_size(self) -> int:
        return self._chunk_size

    def get_destination_path(self) -> Path:
        return self._destination_path
