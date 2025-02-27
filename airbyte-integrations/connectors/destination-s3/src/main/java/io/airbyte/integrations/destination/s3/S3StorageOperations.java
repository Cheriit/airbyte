/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.s3;

import alex.mojaki.s3upload.MultiPartOutputStream;
import alex.mojaki.s3upload.StreamTransferManager;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.ObjectListing;
import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.string.Strings;
import io.airbyte.integrations.destination.NamingConventionTransformer;
import io.airbyte.integrations.destination.record_buffer.SerializableBuffer;
import io.airbyte.integrations.destination.s3.util.S3StreamTransferManagerHelper;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3StorageOperations implements BlobStorageOperations {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3StorageOperations.class);

  private static final int DEFAULT_UPLOAD_THREADS = 10; // The S3 cli uses 10 threads by default.
  private static final int DEFAULT_QUEUE_CAPACITY = DEFAULT_UPLOAD_THREADS;
  private static final int DEFAULT_PART_SIZE = 10;
  private static final int UPLOAD_RETRY_LIMIT = 3;

  private final NamingConventionTransformer nameTransformer;
  private final S3DestinationConfig s3Config;
  private AmazonS3 s3Client;

  public S3StorageOperations(final NamingConventionTransformer nameTransformer, final AmazonS3 s3Client, final S3DestinationConfig s3Config) {
    this.nameTransformer = nameTransformer;
    this.s3Client = s3Client;
    this.s3Config = s3Config;
  }

  @Override
  public String getBucketObjectName(final String namespace, final String streamName) {
    return nameTransformer.applyDefaultCase(String.join("_",
        nameTransformer.convertStreamName(namespace),
        nameTransformer.convertStreamName(streamName)));
  }

  @Override
  public String getBucketObjectPath(final String prefix, final String namespace, final String streamName, final DateTime writeDatetime) {
    return nameTransformer.applyDefaultCase(String.format("%s/%s/%s/%02d/%02d/%02d/",
        prefix,
        getBucketObjectName(namespace, streamName),
        writeDatetime.year().get(),
        writeDatetime.monthOfYear().get(),
        writeDatetime.dayOfMonth().get(),
        writeDatetime.hourOfDay().get()));
  }

  @Override
  public void createBucketObjectIfNotExists(final String objectPath) {
    final String bucket = s3Config.getBucketName();
    if (!s3Client.doesBucketExistV2(bucket)) {
      LOGGER.info("Bucket {} does not exist; creating...", bucket);
      s3Client.createBucket(bucket);
      LOGGER.info("Bucket {} has been created.", bucket);
    }
    if (!s3Client.doesObjectExist(bucket, objectPath)) {
      LOGGER.info("Storage Object {}/{} does not exist in bucket; creating...", bucket, objectPath);
      s3Client.putObject(bucket, objectPath.endsWith("/") ? objectPath : objectPath + "/", "");
      LOGGER.info("Storage Object {}/{} has been created in bucket.", bucket, objectPath);
    }
  }

  @Override
  public String uploadRecordsToBucket(final SerializableBuffer recordsData, final String namespace, final String streamName, final String objectPath)
      throws Exception {
    final List<Exception> exceptionsThrown = new ArrayList<>();
    boolean succeeded = false;
    while (exceptionsThrown.size() < UPLOAD_RETRY_LIMIT && !succeeded) {
      try {
        loadDataIntoBucket(objectPath, recordsData);
        succeeded = true;
      } catch (final Exception e) {
        LOGGER.error("Failed to upload records into storage {}", objectPath, e);
        exceptionsThrown.add(e);
      }
      if (!succeeded) {
        LOGGER.info("Retrying to upload records into storage {} ({}/{}})", objectPath, exceptionsThrown.size(), UPLOAD_RETRY_LIMIT);
        // Force a reconnection before retrying in case error was due to network issues...
        s3Client = s3Config.resetS3Client();
      }
    }
    if (!succeeded) {
      throw new RuntimeException(String.format("Exceptions thrown while uploading records into storage: %s", Strings.join(exceptionsThrown, "\n")));
    }
    return recordsData.getFilename();
  }

  private void loadDataIntoBucket(final String objectPath, final SerializableBuffer recordsData) throws IOException {
    final long partSize = s3Config.getFormatConfig() != null ? s3Config.getFormatConfig().getPartSize() : DEFAULT_PART_SIZE;
    final String bucket = s3Config.getBucketName();
    final String objectKey = String.format("%s%s", objectPath, recordsData.getFilename());
    final StreamTransferManager uploadManager = S3StreamTransferManagerHelper
        .getDefault(bucket, objectKey, s3Client, partSize)
        .checkIntegrity(true)
        .numUploadThreads(DEFAULT_UPLOAD_THREADS)
        .queueCapacity(DEFAULT_QUEUE_CAPACITY);
    boolean hasFailed = false;
    try (final MultiPartOutputStream outputStream = uploadManager.getMultiPartOutputStreams().get(0);
        final InputStream dataStream = recordsData.getInputStream()) {
      dataStream.transferTo(outputStream);
    } catch (final Exception e) {
      LOGGER.error("Failed to load data into storage {}", objectPath, e);
      hasFailed = true;
      throw new RuntimeException(e);
    } finally {
      if (hasFailed) {
        uploadManager.abort();
      } else {
        uploadManager.complete();
      }
    }
    if (!s3Client.doesObjectExist(bucket, objectKey)) {
      LOGGER.error("Failed to upload data into storage, object {} not found", objectKey);
      throw new RuntimeException("Upload failed");
    }
  }

  @Override
  public void dropBucketObject(final String streamName) {
    LOGGER.info("Dropping bucket object {}...", streamName);
    final String bucket = s3Config.getBucketName();
    if (s3Client.doesObjectExist(bucket, streamName)) {
      s3Client.deleteObject(bucket, streamName);
    }
    LOGGER.info("Bucket object {} has been deleted...", streamName);
  }

  @Override
  public void cleanUpBucketObject(final String objectPath, final List<String> stagedFiles) {
    final String bucket = s3Config.getBucketName();
    ObjectListing objects = s3Client.listObjects(bucket, objectPath);
    while (objects.getObjectSummaries().size() > 0) {
      final List<KeyVersion> toDelete = objects.getObjectSummaries()
          .stream()
          .map(obj -> new KeyVersion(obj.getKey()))
          .filter(obj -> stagedFiles.isEmpty() || stagedFiles.contains(obj.getKey()))
          .toList();
      s3Client.deleteObjects(new DeleteObjectsRequest(bucket).withKeys(toDelete));
      LOGGER.info("Storage bucket {} has been cleaned-up ({} objects were deleted)...", objectPath, toDelete.size());
      if (objects.isTruncated()) {
        objects = s3Client.listNextBatchOfObjects(objects);
      } else {
        break;
      }
    }
  }

  @Override
  public boolean isValidData(final JsonNode jsonNode) {
    return true;
  }

}
