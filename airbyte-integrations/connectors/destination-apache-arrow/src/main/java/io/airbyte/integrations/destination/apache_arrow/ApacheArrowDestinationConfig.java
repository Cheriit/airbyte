/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.apache_arrow;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

public class ApacheArrowDestinationConfig {

    protected static final Logger LOGGER = LoggerFactory.getLogger(ApacheArrowDestinationConfig.class);
    private static final String DESTINATION_PATH_FIELD = "destination_path";
    private final Path destinationPath;

    private ApacheArrowDestinationConfig(Path destinationPath) {
        this.destinationPath = destinationPath;
    }

    public static ApacheArrowDestinationConfig of(final JsonNode config) {
        Path destinationPath = getDestinationPath(config);
        return new ApacheArrowDestinationConfig(destinationPath);
    }

    private static Path getDestinationPath(JsonNode config) {
        Path destinationPath = Paths.get(config.get(DESTINATION_PATH_FIELD).asText());
        Preconditions.checkNotNull(destinationPath);

        if (!destinationPath.startsWith("/local")) {
            destinationPath = Path.of("/local", destinationPath.toString());
        }

        final Path normalizePath = destinationPath.normalize();
        if (!normalizePath.startsWith("/local")) {
            throw new IllegalArgumentException("Destination file should be inside the /local directory");
        }
        return destinationPath;
    }

    public Path getDestinationPath() {
        return destinationPath;
    }
}
