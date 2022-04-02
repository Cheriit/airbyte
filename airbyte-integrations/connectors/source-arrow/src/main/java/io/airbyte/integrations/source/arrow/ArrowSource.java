package io.airbyte.integrations.source.arrow;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;

import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.base.Source;
import io.airbyte.protocol.models.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class ArrowSource implements Source {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArrowSource.class);


    public final static void main(String[] args) throws Exception {
        final Source source = new ArrowSource();
        LOGGER.info("starting source: {}", ArrowSource.class);
        new IntegrationRunner(source).run(args);
        LOGGER.info("completed source: {}", ArrowSource.class);

    }

    public ArrowSource(){

    }

    @Override
    public ConnectorSpecification spec() throws Exception {
        final String resourceString = MoreResources.readResource("spec.json");
        return Jsons.deserialize(resourceString, ConnectorSpecification.class);
    }

    @Override
    public AirbyteConnectionStatus check(JsonNode config) throws Exception {
        try {
            final File file = createFile(config);
            if (file.length()>0){
                return new AirbyteConnectionStatus().withStatus(AirbyteConnectionStatus.Status.SUCCEEDED);
            }
            else{
                return new AirbyteConnectionStatus()
                        .withStatus(AirbyteConnectionStatus.Status.FAILED)
                        .withMessage("Could not connect with provided configuration. Error: ");
            }
        }
        catch (final Exception e) {
            LOGGER.info("Exception while checking connection: ", e);
            return new AirbyteConnectionStatus()
                    .withStatus(AirbyteConnectionStatus.Status.FAILED)
                    .withMessage("Could not connect with provided configuration. Error: " + e.getMessage());
        }
    }

    @Override
    public AirbyteCatalog discover(JsonNode config) throws Exception {
            final File file = createFile(config);
            ClientArrowSource cas = getClient(config);
            String name = cas.streamName();
            AirbyteStream as= new AirbyteStream();


            return new AirbyteCatalog();

    }

    private ClientArrowSource getClient(JsonNode config){
        ObjectMapper mapper = new ObjectMapper();
        return new ClientArrowSource(config.get("dataset_name").asText(),
                config.get("dataset_name").asText(),
                mapper.convertValue(config.get("dataset_name"), new TypeReference<Map<String, Object>>(){}),
                config.get("format").asText());
    }

    @Override
    public AutoCloseableIterator<AirbyteMessage> read(JsonNode config, ConfiguredAirbyteCatalog catalog, JsonNode state) throws Exception {
        String fullUrl = "file://"+ config.get("url").asText();
        File file = new File(fullUrl);

        ClientArrowSource cas = getClient(config);
        Iterable fields = selectedFields(catalog);

        try{
            for (var row : cas.read(fields)){
                //TODO
            }
        }

        return AutoCloseableIterators
                .appendOnClose(AutoCloseableIterators.concatWithEagerClose(), () -> {

                });
    }

    public Iterable selectedFields(ConfiguredAirbyteCatalog catalog){
        ArrayList<JsonNode> selectedFields=new ArrayList<>();
        for (ConfiguredAirbyteStream stream :catalog.getStreams()){
            selectedFields.add( stream.getStream().getJsonSchema().get("properties"));
        }
        return selectedFields;
    }




    public File createFile(final JsonNode config) throws SQLException {
        String url = config.get("url").asText();
        ObjectMapper mapper = new ObjectMapper();

        JsonNode prov = config.get("provider");
        Map<String, Object> provider = mapper.convertValue(prov, new TypeReference<Map<String, Object>>(){});

         URLFile fileUrl = new URLFile(url,provider);
        String fullUrl = fileUrl.getFullUrl();
        File file = new File(fullUrl);

        return file;
    }



}
