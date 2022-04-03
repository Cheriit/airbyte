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
    private static final String CHUNK_SIZE_FIELD = "chunk_size";
    private final Path destinationPath;
    private final int chunkSize;

    private ApacheArrowDestinationConfig(Path destinationPath, int chunkSize) {
        this.destinationPath = destinationPath;
        this.chunkSize = chunkSize;
    }

    public static ApacheArrowDestinationConfig of(final JsonNode config) {
        Path destinationPath = getDestinationPath(config);
        int chunkSize = getChunkSize(config);
        return new ApacheArrowDestinationConfig(destinationPath, chunkSize);
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

    private static int getChunkSize(JsonNode config) {
        return config.get(CHUNK_SIZE_FIELD).asInt();
    }

    public Path getDestinationPath() {
        return destinationPath;
    }

    public int getChunkSize() {
        return chunkSize;
    }
}
