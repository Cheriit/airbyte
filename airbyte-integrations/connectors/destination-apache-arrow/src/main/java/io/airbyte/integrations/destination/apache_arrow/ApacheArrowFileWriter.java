package io.airbyte.integrations.destination.apache_arrow;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.integrations.base.JavaBaseConstants;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.*;

public class ApacheArrowFileWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApacheArrowFileWriter.class);
    private final int chunkSize;
    private final RootAllocator allocator;
    private final VectorSchemaRoot schemaRoot;
    private final FileOutputStream outputStream;
    private final ArrowFileWriter fileWriter;
    private final Map<String, String> fieldTypeMap;
    private int chunkIndex;
    private long totalSize;

    public ApacheArrowFileWriter(ApacheArrowDestinationConfig apacheArrowDestinationConfig,
                                 ConfiguredAirbyteStream configuredStream) throws FileNotFoundException {
        JsonNode root = configuredStream.getStream().getJsonSchema();
        File file = apacheArrowDestinationConfig.getDestinationPath().toFile();
        DictionaryProvider.MapDictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider();

        fieldTypeMap = getFieldTypeMap(root);
        Schema schema = getSchema(fieldTypeMap);

        chunkIndex = 0;
        chunkSize = apacheArrowDestinationConfig.getChunkSize();
        allocator = new RootAllocator();
        schemaRoot = VectorSchemaRoot.create(schema, allocator);
        outputStream = new FileOutputStream(file);
        fileWriter = new ArrowFileWriter(schemaRoot, dictProvider, outputStream.getChannel());
    }

    public void write(AirbyteRecordMessage recordMessage) throws IOException {
        LOGGER.info("Start writing");
        if (chunkSize == 0) {
            schemaRoot.allocateNew();
        }

        vectorize(extractData(recordMessage.getData()), chunkIndex, schemaRoot);

        chunkIndex++;
        if (chunkIndex == chunkSize) {
            saveChunk();
        }
    }

    public void close() throws IOException {
        saveChunk();
        fileWriter.end();
        fileWriter.close();
        outputStream.close();
        schemaRoot.close();
        allocator.close();
    }

    private Map<String, String> getFieldTypeMap(JsonNode jsonSchema) {
        Map<String, String> fields = new LinkedHashMap<>();
        var properties = jsonSchema.get("properties").fields();
        while (properties.hasNext()) {
            var property = properties.next();
            String columnName = property.getKey();
            String fieldType = property.getValue().get("type").asText();
            fields.put(columnName, fieldType);
        }
        return fields;
    }

    private Schema getSchema(Map<String, String> fieldTypeMap) {
        List<Field> fieldList = new LinkedList<>();
        fieldList.add(new Field(JavaBaseConstants.COLUMN_NAME_AB_ID, FieldType.nullable(new ArrowType.Utf8()), null));
        // TODO Check if COLUMN_NAME_EMITTED_AT works
        fieldList.add(new Field(JavaBaseConstants.COLUMN_NAME_EMITTED_AT, FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "GMT")), null));

        for (var entry : fieldTypeMap.entrySet()) {
            String columnName = entry.getKey();
            String fieldType = entry.getValue();
            fieldList.add(new Field(columnName, getFieldType(fieldType), null));
        }
        return new Schema(fieldList);
    }

    private FieldType getFieldType(final String dataType) {
        return switch (dataType) {
            case "boolean" -> FieldType.nullable(new ArrowType.Bool());
            case "number" -> FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
            case "array" -> FieldType.nullable(new ArrowType.Utf8());
            case "object" -> FieldType.nullable(new ArrowType.Utf8());
            case "null" -> FieldType.nullable(new ArrowType.Utf8());
            case "string" -> FieldType.nullable(new ArrowType.Utf8());
            default -> FieldType.nullable(new ArrowType.Utf8());
        };
    }

    private Map<String, String> extractData(JsonNode data) {
        Map<String, String> valueMap = new LinkedHashMap<>();
        var fields = data.fields();
        while (fields.hasNext()) {
            var property = fields.next();
            String columnName = property.getKey();
            String fieldType = property.getValue().asText();
            valueMap.put(columnName, fieldType);
        }
        return valueMap;
    }

    private void vectorize(Map<String, String> values, int index, VectorSchemaRoot schemaRoot) {
        ((VarCharVector) schemaRoot.getVector(JavaBaseConstants.COLUMN_NAME_AB_ID)).setSafe(index, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        // TODO Check if COLUMN_NAME_EMITTED_AT works
        ((VarCharVector) schemaRoot.getVector(JavaBaseConstants.COLUMN_NAME_EMITTED_AT)).setSafe(index, LocalDateTime.now().toString().getBytes(StandardCharsets.UTF_8));

        for (var entry : values.entrySet()) {
            String columnName = entry.getKey();
            String fieldValue = entry.getValue();
            switch (fieldTypeMap.get(columnName)) {
                case "boolean":
                    ((BitVector) schemaRoot.getVector(columnName)).setSafe(index, Boolean.parseBoolean(fieldValue) ? 1 : 0);
                    break;
                case "number":
                    ((Float8Vector) schemaRoot.getVector(columnName)).setSafe(index, Double.parseDouble(fieldValue));
                    break;
                case "string":
                case "array":
                case "object":
                case "null":
                default:
                    ((VarCharVector) schemaRoot.getVector(columnName)).setSafe(index, fieldValue.getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    private void saveChunk() throws IOException {
        totalSize = totalSize + chunkIndex;
        LOGGER.info("Filled chunk with {} items; {} items written", chunkIndex, totalSize);
        schemaRoot.setRowCount(chunkIndex);
        LOGGER.info("Chunk written");
        fileWriter.writeBatch();
        schemaRoot.clear();
        chunkIndex = 0;
    }
}
