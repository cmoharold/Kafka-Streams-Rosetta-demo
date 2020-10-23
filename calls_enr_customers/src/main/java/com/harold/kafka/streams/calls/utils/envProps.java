package com.harold.kafka.streams.calls.utils;

public class envProps {
    public static final String INPUT_TOPIC_CALLS = "INPUT_TOPIC_CALLS";
    public static final String INPUT_TOPIC_CUSTOMERS = "INPUT_TOPIC_CUSTOMERS";
    public static final String OUTPUT_TOPIC = "OUTPUT_TOPIC";
    public static final String GROUP_ID_CONFIG = "GROUP_ID_CONFIG";
    public static final String AUTO_OFFSET_RESET_CONFIG = "AUTO_OFFSET_RESET_CONFIG";
    public static final String APPLICATION_ID_CONFIG = "APPLICATION_ID_CONFIG";
    public static final String BOOTSTRAP_SERVERS_CONFIG = "BOOTSTRAP_SERVERS_CONFIG";
    public static final String NUM_STREAM_THREADS_CONFIG = "NUM_STREAM_THREADS_CONFIG";
    public static final String SCHEMA_REGISTRY_URL_CONFIG = "SCHEMA_REGISTRY_URL_CONFIG";

    public static String getEnvValue(String environmentKey, String defaultValue)
    {
        String envValue = System.getenv(environmentKey);
        if(envValue != null && !envValue.isEmpty())
        {
            return envValue;
        }
        return defaultValue;
    }
}