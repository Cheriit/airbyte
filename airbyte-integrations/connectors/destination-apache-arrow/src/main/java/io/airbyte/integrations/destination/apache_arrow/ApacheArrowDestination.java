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
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApacheArrowDestination extends BaseConnector implements Destination {

  private static final Logger LOGGER = LoggerFactory.getLogger(ApacheArrowDestination.class);

  public ApacheArrowDestination() {
    // TODO setup needed classes
  }

  /**
   * @param config - JsonNode containing config required to Apache Arrow destination
   * @return AirbyteConnectionStatus - status describind if config is valid
   */
  @Override
  public AirbyteConnectionStatus check(JsonNode config) {
    // TODO check if a given config is valid
    // Example:
    //    try {
    //      FileUtils.forceMkdir(getDestinationPath(config).toFile());
    //    } catch (final Exception e) {
    //      return new AirbyteConnectionStatus().withStatus(Status.FAILED).withMessage(e.getMessage());
    //    }
    //    return new AirbyteConnectionStatus().withStatus(Status.SUCCEEDED);

    return null;
  }

  /**
   * @param config - Apache Arrow destination config.
   * @param catalog - schema of the incoming messages.
   * @return - a consumer to handle writing records to the Apache Arrow.
   * @throws IOException - exception throw in manipulating the Apache Arrow.
   */
  @Override
  public AirbyteMessageConsumer getConsumer(JsonNode config,
                                            ConfiguredAirbyteCatalog configuredCatalog,
                                            Consumer<AirbyteMessage> outputRecordCollector) {
    // TODO generate ApacheArrowConsumer with all of its needed parameters
    return new ApacheArrowConsumer();
  }

  public static void main(String[] args) throws Exception {
    final Destination destination = new ApacheArrowDestination();
    LOGGER.info("Starting destination: {}", ApacheArrowDestination.class);
    new IntegrationRunner(destination).run(args);
    LOGGER.info("Completed destination: {}", ApacheArrowDestination.class);
    );
  }

}
