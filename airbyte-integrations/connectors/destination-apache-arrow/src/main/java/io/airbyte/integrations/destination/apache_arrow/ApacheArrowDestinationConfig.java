public class ApacheArrowDestinationConfig {

    protected static final Logger LOGGER = LoggerFactory.getLogger(ApacheArrowDestinationConfig.class);

    private ApacheArrowDestinationConfig() {
        // TODO Add fields to config class
    }

    public static ApacheArrowDestinationConfig getApacheArrowDestinationConfig(final JsonNode config) {
        return new ApacheArrowDestinationConfig()();
    }
}