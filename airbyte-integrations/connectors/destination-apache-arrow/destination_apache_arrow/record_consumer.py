#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from typing import Iterable

from airbyte_cdk import logger
from airbyte_cdk.entrypoint import logger
from airbyte_cdk.models import AirbyteMessage, ConfiguredAirbyteCatalog, Type
from destination_apache_arrow.config import DestinationApacheArrowConfig
from destination_apache_arrow.file_writer import DestinationApacheArrowFileWriter
from destination_apache_arrow.invalid_message_type_error import InvalidMessageTypeError
from destination_apache_arrow.invalid_stream_name_error import InvalidStreamNameError


class DestinationApacheArrowRecordConsumer:
    config: DestinationApacheArrowConfig
    writers: dict[str, DestinationApacheArrowFileWriter]

    def __init__(self, config: DestinationApacheArrowConfig, configured_catalog: ConfiguredAirbyteCatalog):
        self.config = config
        self.writers = dict()
        for configured_stream in configured_catalog.streams:
            stream_name = configured_stream.stream.name
            self.writers[stream_name] = DestinationApacheArrowFileWriter(config, configured_stream)
            # if configured_stream.destination_sync_mode == DestinationSyncMode.overwrite:
            #     self.writers[stream_name].delete_stream_from_entries(stream_name)

    def accept(self, input_messages: Iterable[AirbyteMessage]) -> Iterable[AirbyteMessage]:
        counter = 0
        try:
            for message in input_messages:
                counter += 1
                record = message.record
                writer = self.writers[record.stream]

                if writer is None:
                    message = f"Unexpected stream name: {record.stream}"
                    logger.error(message)
                    raise InvalidStreamNameError(message)

                if message.type == Type.STATE:
                    logger.info(f'State: {counter}')
                    writer.flush()
                    yield message
                elif message.type == Type.RECORD:
                    record = message.record
                    writer.write(record)
                else:
                    message = f"Unexpected message type: {message.type}"
                    logger.error(message)
                    raise InvalidMessageTypeError(message)
            logger.error(f"Loop end: {counter}")
            self.close()
        except Exception as err:
            message = f"Failed to write data to {self.config.get_destination_path()}: {repr(err)}"
            logger.error(message)
            raise err

    def close(self):
        for writer in self.writers.values():
            writer.close()
