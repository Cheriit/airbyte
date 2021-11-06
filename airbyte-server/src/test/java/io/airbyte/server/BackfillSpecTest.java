/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.Configs.WorkerEnvironment;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.helpers.LogConfigs;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.scheduler.client.SynchronousJobMetadata;
import io.airbyte.scheduler.client.SynchronousResponse;
import io.airbyte.scheduler.client.SynchronousSchedulerClient;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BackfillSpecTest {

  private static final String SOURCE_DOCKER_REPO = "docker-repo/source";
  private static final String DEST_DOCKER_REPO = "docker-repo/destination";
  private static final String DOCKER_IMAGE_TAG = "tag";
  private static final StandardWorkspace WORKSPACE = new StandardWorkspace().withWorkspaceId(UUID.randomUUID());

  private ConfigRepository configRepository;
  private SynchronousSchedulerClient schedulerClient;

  @BeforeEach
  void setup() throws IOException, JsonValidationException {
    configRepository = mock(ConfigRepository.class);
    when(configRepository.listStandardWorkspaces(true)).thenReturn(List.of(WORKSPACE));

    schedulerClient = mock(SynchronousSchedulerClient.class);
  }

  @Test
  public void testBackfillSpecSuccessful() throws Exception {
    final StandardSourceDefinition sourceDef = new StandardSourceDefinition().withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(DOCKER_IMAGE_TAG);
    final StandardDestinationDefinition destDef = new StandardDestinationDefinition().withDockerRepository(DEST_DOCKER_REPO)
        .withDockerImageTag(DOCKER_IMAGE_TAG);

    when(configRepository.listStandardSourceDefinitions()).thenReturn(List.of(sourceDef));
    when(configRepository.listStandardDestinationDefinitions()).thenReturn(List.of(destDef));

    final ConnectorSpecification sourceSpec = new ConnectorSpecification().withDocumentationUrl(URI.create("http://source.org"));
    final ConnectorSpecification destSpec = new ConnectorSpecification().withDocumentationUrl(URI.create("http://dest.org"));

    final SynchronousResponse<ConnectorSpecification> successfulSourceResponse = new SynchronousResponse<>(
        sourceSpec,
        mockJobMetadata(true));
    final SynchronousResponse<ConnectorSpecification> successfulDestResponse = new SynchronousResponse<>(
        destSpec,
        mockJobMetadata(true));

    final SynchronousSchedulerClient schedulerClient = mock(SynchronousSchedulerClient.class);
    when(schedulerClient.createGetSpecJob(SOURCE_DOCKER_REPO + ":" + DOCKER_IMAGE_TAG)).thenReturn(successfulSourceResponse);
    when(schedulerClient.createGetSpecJob(DEST_DOCKER_REPO + ":" + DOCKER_IMAGE_TAG)).thenReturn(successfulDestResponse);

    ServerApp.migrateAllDefinitionsToContainSpec(
        configRepository,
        schedulerClient,
        WorkerEnvironment.DOCKER,
        mock(LogConfigs.class),
        true);

    final StandardSourceDefinition expectedSourceDef = Jsons.clone(sourceDef).withSpec(sourceSpec);
    final StandardDestinationDefinition expectedDestDef = Jsons.clone(destDef).withSpec(destSpec);
    verify(configRepository, times(1)).writeStandardSourceDefinition(expectedSourceDef);
    verify(configRepository, times(1)).writeStandardDestinationDefinition(expectedDestDef);
    verify(configRepository, never()).deleteSourceDefinition(sourceDef.getSourceDefinitionId());
    verify(configRepository, never()).deleteDestinationDefinition(destDef.getDestinationDefinitionId());
  }

  @Test
  public void testBackfillSpecFailureWithDeletion() throws Exception {
    final StandardSourceDefinition sourceDef = new StandardSourceDefinition().withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(DOCKER_IMAGE_TAG);
    final StandardDestinationDefinition destDef = new StandardDestinationDefinition().withDockerRepository(DEST_DOCKER_REPO)
        .withDockerImageTag(DOCKER_IMAGE_TAG);

    when(configRepository.listStandardSourceDefinitions()).thenReturn(List.of(sourceDef));
    when(configRepository.listStandardDestinationDefinitions()).thenReturn(List.of(destDef));

    final ConnectorSpecification sourceSpec = new ConnectorSpecification().withDocumentationUrl(URI.create("http://source.org"));
    final ConnectorSpecification destSpec = new ConnectorSpecification().withDocumentationUrl(URI.create("http://dest.org"));

    final SynchronousResponse<ConnectorSpecification> failureSourceResponse = new SynchronousResponse<>(
        sourceSpec,
        mockJobMetadata(false));
    final SynchronousResponse<ConnectorSpecification> failureDestResponse = new SynchronousResponse<>(
        destSpec,
        mockJobMetadata(false));

    when(schedulerClient.createGetSpecJob(SOURCE_DOCKER_REPO + ":" + DOCKER_IMAGE_TAG)).thenReturn(failureSourceResponse);
    when(schedulerClient.createGetSpecJob(DEST_DOCKER_REPO + ":" + DOCKER_IMAGE_TAG)).thenReturn(failureDestResponse);

    ServerApp.migrateAllDefinitionsToContainSpec(
        configRepository,
        schedulerClient,
        WorkerEnvironment.DOCKER,
        mock(LogConfigs.class),
        true);

    verify(configRepository, never()).writeStandardSourceDefinition(any());
    verify(configRepository, never()).writeStandardDestinationDefinition(any());
    verify(configRepository, never()).writeStandardSourceDefinition(any());
    verify(configRepository, never()).writeStandardDestinationDefinition(any());
    verify(configRepository).deleteSourceDefinition(sourceDef.getSourceDefinitionId());
    verify(configRepository).deleteDestinationDefinition(destDef.getDestinationDefinitionId());
  }

  @Test
  public void testBackfillSpecFailureThrow() throws Exception {
    final StandardSourceDefinition sourceDef = new StandardSourceDefinition().withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(DOCKER_IMAGE_TAG);
    final StandardDestinationDefinition destDef = new StandardDestinationDefinition().withDockerRepository(DEST_DOCKER_REPO)
        .withDockerImageTag(DOCKER_IMAGE_TAG);

    when(configRepository.listStandardSourceDefinitions()).thenReturn(List.of(sourceDef));
    when(configRepository.listStandardDestinationDefinitions()).thenReturn(List.of(destDef));

    final ConnectorSpecification sourceSpec = new ConnectorSpecification().withDocumentationUrl(URI.create("http://source.org"));
    final ConnectorSpecification destSpec = new ConnectorSpecification().withDocumentationUrl(URI.create("http://dest.org"));

    final SynchronousResponse<ConnectorSpecification> failureSourceResponse = new SynchronousResponse<>(
        sourceSpec,
        mockJobMetadata(false));
    final SynchronousResponse<ConnectorSpecification> failureDestResponse = new SynchronousResponse<>(
        destSpec,
        mockJobMetadata(false));

    when(schedulerClient.createGetSpecJob(SOURCE_DOCKER_REPO + ":" + DOCKER_IMAGE_TAG)).thenReturn(failureSourceResponse);
    when(schedulerClient.createGetSpecJob(DEST_DOCKER_REPO + ":" + DOCKER_IMAGE_TAG)).thenReturn(failureDestResponse);

    assertThrows(RuntimeException.class, () -> ServerApp.migrateAllDefinitionsToContainSpec(
        configRepository,
        schedulerClient,
        WorkerEnvironment.DOCKER,
        mock(LogConfigs.class),
        false));

    verify(configRepository, never()).writeStandardSourceDefinition(any());
    verify(configRepository, never()).writeStandardDestinationDefinition(any());
    verify(configRepository, never()).writeStandardSourceDefinition(any());
    verify(configRepository, never()).writeStandardDestinationDefinition(any());
    verify(configRepository, never()).deleteSourceDefinition(any());
    verify(configRepository, never()).deleteDestinationDefinition(any());
  }

  @Test
  public void testSpecAlreadyExists() throws Exception {
    final ConnectorSpecification sourceSpec = new ConnectorSpecification().withDocumentationUrl(URI.create("http://source.org"));
    final ConnectorSpecification destSpec = new ConnectorSpecification().withDocumentationUrl(URI.create("http://dest.org"));
    final StandardSourceDefinition sourceDef = new StandardSourceDefinition().withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(DOCKER_IMAGE_TAG).withSpec(sourceSpec);
    final StandardDestinationDefinition destDef = new StandardDestinationDefinition().withDockerRepository(DEST_DOCKER_REPO)
        .withDockerImageTag(DOCKER_IMAGE_TAG).withSpec(destSpec);

    when(configRepository.listStandardSourceDefinitions()).thenReturn(List.of(sourceDef));
    when(configRepository.listStandardDestinationDefinitions()).thenReturn(List.of(destDef));

    ServerApp.migrateAllDefinitionsToContainSpec(
        configRepository,
        mock(SynchronousSchedulerClient.class),
        WorkerEnvironment.DOCKER,
        mock(LogConfigs.class),
        true);

    verify(schedulerClient, never()).createGetSpecJob(any());
    verify(configRepository, never()).writeStandardSourceDefinition(any());
    verify(configRepository, never()).writeStandardDestinationDefinition(any());
    verify(configRepository, never()).deleteSourceDefinition(sourceDef.getSourceDefinitionId());
    verify(configRepository, never()).deleteDestinationDefinition(destDef.getDestinationDefinitionId());
  }

  private SynchronousJobMetadata mockJobMetadata(final boolean succeeded) {
    final long now = Instant.now().toEpochMilli();
    return new SynchronousJobMetadata(
        UUID.randomUUID(),
        ConfigType.GET_SPEC,
        UUID.randomUUID(),
        now,
        now,
        succeeded,
        Path.of("path", "to", "logs"));
  }

}
