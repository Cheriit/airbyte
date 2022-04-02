/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.apache_arrow;

import io.airbyte.integrations.base.FailureTrackingAirbyteMessageConsumer;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class ApacheArrowRecordConsumer extends FailureTrackingAirbyteMessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApacheArrowRecordConsumer.class);
    private final ConfiguredAirbyteCatalog catalog;

    public ApacheArrowRecordConsumer(final ApacheArrowDestinationConfig apacheArrowDestinationConfig,
                                     final ConfiguredAirbyteCatalog catalog,
                                     final Consumer<AirbyteMessage> outputRecordCollector) {
        this.catalog = catalog;
        LOGGER.info("Initializing ApacheArrowRecordConsumer.");
    }

    @Override
    protected void startTracked() throws Exception {

    }

    @Override
    protected void acceptTracked(final AirbyteMessage airbyteMessage) throws Exception {

    }

    @Override
    protected void close(final boolean hasFailed) throws Exception {

    }

}
