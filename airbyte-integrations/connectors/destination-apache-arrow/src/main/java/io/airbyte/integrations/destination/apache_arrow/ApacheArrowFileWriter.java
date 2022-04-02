package io.airbyte.integrations.destination.apache_arrow;

import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;

import java.util.UUID;

public class ApacheArrowFileWriter {

    public ApacheArrowFileWriter(ApacheArrowDestinationConfig apacheArrowDestinationConfig,
                                 ConfiguredAirbyteStream configuredStream) {

    }

    public void write(AirbyteRecordMessage recordMessage) {
        UUID.randomUUID();
    }

    public void close(boolean hasFailed) {
    }


}
