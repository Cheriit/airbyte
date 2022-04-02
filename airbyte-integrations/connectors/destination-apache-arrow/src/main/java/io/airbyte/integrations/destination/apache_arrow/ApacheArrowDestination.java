/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.apache_arrow;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.integrations.BaseConnector;
import io.airbyte.integrations.base.AirbyteMessageConsumer;
import io.airbyte.integrations.base.Destination;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteConnectionStatus.Status;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class ApacheArrowDestination extends BaseConnector implements Destination {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApacheArrowDestination.class);

    public ApacheArrowDestination() {
    }

    public static void main(String[] args) throws Exception {
        final Destination destination = new ApacheArrowDestination();
        LOGGER.info("Starting destination: {}", ApacheArrowDestination.class);
        new IntegrationRunner(destination).run(args);
        LOGGER.info("Completed destination: {}", ApacheArrowDestination.class);
    }

    /**
     * @param config - JsonNode containing config required to Apache Arrow destination
     * @return AirbyteConnectionStatus - status describing if config is valid
     */
    @Override
    public AirbyteConnectionStatus check(JsonNode config) {
        try {
            ApacheArrowDestinationConfig arrowConfig = ApacheArrowDestinationConfig.of(config);
            FileUtils.forceMkdir(arrowConfig.getDestinationPath().toFile());
            LOGGER.info("Successfully finished ApacheArrowDestination::check");
        } catch (final Exception e) {
            LOGGER.error("Exception in ApacheArrowDestination::check");
            return new AirbyteConnectionStatus().withStatus(Status.FAILED).withMessage(e.getMessage());
        }
        return new AirbyteConnectionStatus().withStatus(Status.SUCCEEDED);
    }

    /**
     * @param config                - Apache Arrow destination config.
     * @param catalog               - schema of the incoming messages.
     * @param outputRecordCollector - TODO.
     * @return - a consumer to handle writing records to the Apache Arrow.
     */
    @Override
    public AirbyteMessageConsumer getConsumer(JsonNode config,
                                              ConfiguredAirbyteCatalog catalog,
                                              Consumer<AirbyteMessage> outputRecordCollector) {
        ApacheArrowDestinationConfig arrowConfig = ApacheArrowDestinationConfig.of(config);
        return new ApacheArrowRecordConsumer(arrowConfig, catalog, outputRecordCollector);
    }

}
