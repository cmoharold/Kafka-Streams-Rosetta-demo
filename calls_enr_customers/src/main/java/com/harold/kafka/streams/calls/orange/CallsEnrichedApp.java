package com.harold.kafka.streams.calls.orange;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.text.SimpleDateFormat;

import com.harold.kafka.streams.calls.utils.envProps;

public class CallsEnrichedApp {

    static final String clientId = envProps.getEnvValue(envProps.APPLICATION_ID_CONFIG, "calls-orange");
    static final String groupId = envProps.getEnvValue(envProps.GROUP_ID_CONFIG, "rosetta");
    static final String endpoints = envProps.getEnvValue(envProps.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    static final String table_calls = envProps.getEnvValue(envProps.INPUT_TOPIC_CALLS, "CALLS_AGG");
    static final String table_clientes = envProps.getEnvValue(envProps.INPUT_TOPIC_CUSTOMERS, "CLIENTES_PORTA_SCR_T");
    static final String stream_call_clientes = envProps.getEnvValue(envProps.OUTPUT_TOPIC, "CALLS_CLIENTES_ENR");
    static final String autoOffsetResetPolicy = envProps.getEnvValue(envProps.AUTO_OFFSET_RESET_CONFIG, "earliest");
    static final String streamsNumOfThreads = envProps.getEnvValue(envProps.NUM_STREAM_THREADS_CONFIG, "3");
    static final String schemaRegistryUrl = envProps.getEnvValue(envProps.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
    //static final String schemaRegistryUrl = "mock://com.harold.kafka.streams.calls.orange.CallsEnrichedAppTest";
    static final String keySerde = "org.apache.kafka.common.serialization.Serdes$StringSerde";
    //static final String valueSerde = GenericAvroSerde.class.getCanonicalName();
    static final String deserializationExceptionHandler = LogAndContinueExceptionHandler.class.getCanonicalName();

    private static Properties createProperties() {
        Properties config = new Properties();
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetPolicy);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, clientId);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, keySerde);
        //config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerde);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, endpoints);
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, streamsNumOfThreads);
        config.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, deserializationExceptionHandler);
        config.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        return config;
    }

    public Topology createTopology() throws IOException {
        // json Serde
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
        // string Serde
        final Serde<String> stringSerde = Serdes.String();
        // value avro Serde
        final Serde<GenericRecord> valueAvroSerde = new GenericAvroSerde();
        valueAvroSerde.configure(getSerdeProperties(), false);

        final InputStream
                callsAggregateSchema =
                CallsEnrichedApp.class.getClassLoader()
                        .getResourceAsStream("callaggcust.avsc");
        final Schema schema = new Schema.Parser().parse(callsAggregateSchema);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, JsonNode> customerEnr = builder.stream(table_clientes, Consumed.with(stringSerde, jsonSerde));

        KStream<String, GenericRecord> callAgg = builder.stream(table_calls, Consumed.with(stringSerde, valueAvroSerde));

        KStream<String, GenericRecord> callAggRekeyed = callAgg
                .selectKey((key, value) -> value.get("ID_TELEF_ORIGEN").toString());

        // create the initial json object for balances
        ObjectNode initialCustomer = JsonNodeFactory.instance.objectNode();
        initialCustomer.put("doc_cliente", "");
        initialCustomer.put("cliente_orange", 0);
        initialCustomer.put("days_excliente", 0);
        initialCustomer.put("operador_actual", "");
        initialCustomer.put("riesgo", "");

        KTable<String, JsonNode> customerEnrRekeyed = customerEnr
                .selectKey((key, value) -> value.get("TELEFONO").asText())
                .groupByKey()
                .aggregate(
                        () -> initialCustomer,
                        (key, value, aggregate) -> value,
                        Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as("customer_enr_rekey")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(jsonSerde)
                );

        KStream<String, GenericRecord> callsCustomerJoin = callAggRekeyed
                .leftJoin(customerEnrRekeyed, (call, customer) -> {
                    final GenericRecord callCustomer = new GenericData.Record(schema);
                    callCustomer.put("id_telef_origen", call.get("ID_TELEF_ORIGEN").toString());
                    callCustomer.put("window_start_ts", getReadableDate(new Long(call.get("WINDOW_START_TS").toString())));
                    callCustomer.put("window_end_ts", getReadableDatePlusOneHour(new Long(call.get("WINDOW_START_TS").toString())));
                    callCustomer.put("calls_count", call.get("CALLS_COUNT"));
                    callCustomer.put("max_duracion_origen", call.get("MAX_DURACION_ORIGEN"));
                    callCustomer.put("total_duracion_origen", call.get("TOTAL_DURACION_ORIGEN"));
                    callCustomer.put("avg_duracion_origen", call.get("AVG_DURACION_ORIGEN"));
                    callCustomer.put("doc_cliente", customer == null ? null : customer.get("DOC_CLIENTE").asText());
                    callCustomer.put("cliente_orange", customer == null ? null : customer.get("CLIENTE_ORANGE").asInt());
                    callCustomer.put("days_excliente", customer == null ? null : customer.get("DAYS_EXCLIENTE").asInt());
                    callCustomer.put("operador_actual", customer == null ? null : customer.get("OPERADOR_ACTUAL").asText());
                    callCustomer.put("riesgo", customer == null ? null : customer.get("RIESGO").asText());
                    return callCustomer;
                });

        callsCustomerJoin.to(stream_call_clientes, Produced.with(stringSerde,valueAvroSerde));

        return builder.build();
    }

    public static void main(String[] args) throws IOException {

        Properties config = createProperties();
        CallsEnrichedApp callsEnrichedApp = new CallsEnrichedApp();
        KafkaStreams streams = new KafkaStreams(callsEnrichedApp.createTopology(), config);
        streams.cleanUp();
        streams.start();

        // print the topology
        streams.localThreadsMetadata().forEach(data -> System.out.println(data));

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Map<String, String> getSerdeProperties() {
        return Collections.singletonMap(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    }

    public String getReadableDate(Long epoch) {
        String date = new SimpleDateFormat("dd/MM/yyyy HH:mm").format(new Date(epoch));
        return date;
    }

    public String getReadableDatePlusOneHour(Long epoch) {
        String date = new SimpleDateFormat("dd/MM/yyyy HH:mm").format(DateUtils.addHours(new Date(epoch),1));
        return date;
    }

}