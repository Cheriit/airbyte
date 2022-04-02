/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

public class ApacheArrowRecordConsumer extends FailureTrackingAirbyteMessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApacheArrowRecordConsumer.class);

    private final ConfiguredAirbyteCatalog catalog;

    public ApacheArrowRecordConsumer(final ApacheArrowDestinationConfig apacheArrowDestinationConfig,
                                     final ConfiguredAirbyteCatalog catalog,
                                     final Consumer<AirbyteMessage> outputRecordCollector) {
        super(outputRecordCollector);
        this.catalog = catalog;
        LOGGER.info("initializing consumer.");
    }
}