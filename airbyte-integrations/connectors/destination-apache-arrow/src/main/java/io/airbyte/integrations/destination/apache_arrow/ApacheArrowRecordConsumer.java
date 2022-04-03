/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.apache_arrow;

import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.string.Strings;
import io.airbyte.integrations.base.AirbyteStreamNameNamespacePair;
import io.airbyte.integrations.base.FailureTrackingAirbyteMessageConsumer;
import io.airbyte.protocol.models.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

public class ApacheArrowRecordConsumer extends FailureTrackingAirbyteMessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApacheArrowRecordConsumer.class);
    private final ApacheArrowDestinationConfig apacheArrowDestinationConfig;
    private final ConfiguredAirbyteCatalog configuredCatalog;
    private final Consumer<AirbyteMessage> outputRecordCollector;
    private final Map<AirbyteStreamNameNamespacePair, ApacheArrowFileWriter> streamNameAndNamespaceToWriters;

    private AirbyteMessage lastStateMessage = null;

    public ApacheArrowRecordConsumer(final ApacheArrowDestinationConfig apacheArrowDestinationConfig,
                                     final ConfiguredAirbyteCatalog configuredCatalog,
                                     final Consumer<AirbyteMessage> outputRecordCollector) {
        this.apacheArrowDestinationConfig = apacheArrowDestinationConfig;
        this.configuredCatalog = configuredCatalog;
        this.outputRecordCollector = outputRecordCollector;
        this.streamNameAndNamespaceToWriters = new HashMap<>(configuredCatalog.getStreams().size());
        LOGGER.info("Initialized ApacheArrowRecordConsumer.");
    }

    @Override
    protected void startTracked() throws Exception {
        for (final ConfiguredAirbyteStream configuredStream : configuredCatalog.getStreams()) {
            final ApacheArrowFileWriter writer = new ApacheArrowFileWriter(apacheArrowDestinationConfig, configuredStream);
            final AirbyteStream stream = configuredStream.getStream();
            final AirbyteStreamNameNamespacePair streamNamePair = AirbyteStreamNameNamespacePair.fromAirbyteSteam(stream);
            streamNameAndNamespaceToWriters.put(streamNamePair, writer);
        }
    }

    @Override
    protected void acceptTracked(final AirbyteMessage airbyteMessage) throws Exception {
        if (airbyteMessage.getType() == AirbyteMessage.Type.STATE) {
            lastStateMessage = airbyteMessage;
        } else if (airbyteMessage.getType() == AirbyteMessage.Type.RECORD) {
            final AirbyteRecordMessage recordMessage = airbyteMessage.getRecord();
            final AirbyteStreamNameNamespacePair pair = AirbyteStreamNameNamespacePair.fromRecordMessage(recordMessage);

            if (!streamNameAndNamespaceToWriters.containsKey(pair)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Message contained record from a stream that was not in the catalog.%n catalog: %s ,%n message: %s",
                                Jsons.serialize(configuredCatalog), Jsons.serialize(recordMessage)));
            }

            streamNameAndNamespaceToWriters.get(pair).write(recordMessage);
        } else {
            LOGGER.warn("Unexpected message type: {}", airbyteMessage.getType());
        }
    }

    @Override
    protected void close(final boolean hasFailed) throws Exception {
        LOGGER.debug("Closing consumer with writers = {}", streamNameAndNamespaceToWriters);
        List<Exception> exceptionsThrown = new ArrayList<>();
        for (var entry : streamNameAndNamespaceToWriters.entrySet()) {
            final ApacheArrowFileWriter handler = entry.getValue();
            LOGGER.debug("Closing writer {}", entry.getKey());
            try {
                handler.close();
            } catch (Exception e) {
                exceptionsThrown.add(e);
                LOGGER.error("Exception while closing writer {}", entry.getKey(), e);
            }
        }

        if (!exceptionsThrown.isEmpty()) {
            throw new RuntimeException(String.format("Exceptions thrown while closing consumer: %s", Strings.join(exceptionsThrown, "\n")));
        }

        if (!hasFailed) {
            outputRecordCollector.accept(lastStateMessage);
        }
    }
}
