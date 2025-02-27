/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.s3.csv;

import io.airbyte.commons.functional.CheckedBiFunction;
import io.airbyte.integrations.base.AirbyteStreamNameNamespacePair;
import io.airbyte.integrations.destination.record_buffer.BaseSerializedBuffer;
import io.airbyte.integrations.destination.record_buffer.BufferStorage;
import io.airbyte.integrations.destination.record_buffer.SerializableBuffer;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;

public class CsvSerializedBuffer extends BaseSerializedBuffer {

  private final CsvSheetGenerator csvSheetGenerator;
  private CSVPrinter csvPrinter;
  private CSVFormat csvFormat;

  protected CsvSerializedBuffer(final BufferStorage bufferStorage, final CsvSheetGenerator csvSheetGenerator) throws Exception {
    super(bufferStorage);
    this.csvSheetGenerator = csvSheetGenerator;
    this.csvPrinter = null;
    this.csvFormat = CSVFormat.DEFAULT;
    // we always want to compress csv files
    withCompression(true);
  }

  public CsvSerializedBuffer withCsvFormat(final CSVFormat csvFormat) {
    if (csvPrinter == null) {
      this.csvFormat = csvFormat;
      return this;
    }
    throw new RuntimeException("Options should be configured before starting to write");
  }

  @Override
  protected void createWriter(final OutputStream outputStream) throws IOException {
    csvPrinter = new CSVPrinter(new PrintWriter(outputStream, true, StandardCharsets.UTF_8), csvFormat);
  }

  @Override
  protected void writeRecord(final AirbyteRecordMessage recordMessage) throws IOException {
    csvPrinter.printRecord(csvSheetGenerator.getDataRow(UUID.randomUUID(), recordMessage));
  }

  @Override
  protected void closeWriter() throws IOException {
    csvPrinter.flush();
    csvPrinter.close();
  }

  public static CheckedBiFunction<AirbyteStreamNameNamespacePair, ConfiguredAirbyteCatalog, SerializableBuffer, Exception> createFunction(
                                                                                                                                          final S3CsvFormatConfig config,
                                                                                                                                          final Callable<BufferStorage> createStorageFunction) {
    return (final AirbyteStreamNameNamespacePair stream, final ConfiguredAirbyteCatalog catalog) -> {
      final CsvSheetGenerator csvSheetGenerator;
      if (config != null) {
        csvSheetGenerator = CsvSheetGenerator.Factory.create(catalog.getStreams()
            .stream()
            .filter(s -> s.getStream().getName().equals(stream.getName()) && StringUtils.equals(s.getStream().getNamespace(), stream.getNamespace()))
            .findFirst()
            .orElseThrow(() -> new RuntimeException(String.format("No such stream %s.%s", stream.getNamespace(), stream.getName())))
            .getStream()
            .getJsonSchema(),
            config);
      } else {
        csvSheetGenerator = new StagingDatabaseCsvSheetGenerator();
      }
      return new CsvSerializedBuffer(createStorageFunction.call(), csvSheetGenerator);
    };
  }

}
