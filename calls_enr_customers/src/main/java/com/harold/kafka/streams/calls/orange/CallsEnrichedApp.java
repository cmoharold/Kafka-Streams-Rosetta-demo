package com.harold.kafka.streams.calls.orange;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
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
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Date;
import java.text.SimpleDateFormat;

//import org.apache.kafka.streams.Consumed;

public class CallsEnrichedApp {

    private static String clientId = "calls-orange";
    private static String groupId = "rosetta";
    private static String endpoints = "localhost:9092";
    private static String table_calls = "CALLS_AGG";
    private static String table_clientes = "CLIENTES_PORTA_SCR_T";
    private static String autoOffsetResetPolicy = "earliest";
    private static String streamsNumOfThreads = "3";
    private static String schemaRegistryUrl = "http://localhost:8081";
    private static String keySerde = "org.apache.kafka.common.serialization.Serdes$StringSerde";
    private static String valueSerde = GenericAvroSerde.class.getCanonicalName();
    private static String deserializationExceptionHandler = LogAndContinueExceptionHandler.class.getCanonicalName();

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
                        .getResourceAsStream("callagg.avsc");
        final Schema schema = new Schema.Parser().parse(callsAggregateSchema);

        StreamsBuilder builder = new StreamsBuilder();
        
        KTable<String, JsonNode> customerEnr = builder.table(table_clientes, Consumed.with(stringSerde, jsonSerde));

        KTable<String, GenericRecord> callAgg = builder.table(table_calls, Consumed.with(stringSerde, valueAvroSerde));

        KTable<String, GenericRecord> callAggRekeyed = callAgg
                .toStream()
                .selectKey((key, value) -> value.get("ID_TELEF_ORIGEN").toString())
                //.map((k, v) -> new KeyValue<>(v.get("ID_TELEF_ORIGEN").toString(), v))
                .groupByKey()
                //.groupBy((k, v) -> KeyValue.pair((String)v.get("id_telef_origen"), v))
                .aggregate(
                        () -> null,
                        (key, value, aggregate) -> value,
                        Materialized.with(stringSerde,valueAvroSerde));

        // create the initial json object for balances
        ObjectNode initialCustomer = JsonNodeFactory.instance.objectNode();
        initialCustomer.put("doc_cliente", "");
        initialCustomer.put("cliente_orange", 0);
        initialCustomer.put("days_excliente", 0);
        initialCustomer.put("operador_actual", "");
        initialCustomer.put("riesgo", "");

        KTable<String, JsonNode> customerEnrRekeyed = customerEnr
                .toStream()
                .selectKey((key, value) -> value.get("TELEFONO").asText())
                //.map((k, v) -> new KeyValue<>(v.get("TELEFONO").asText(), v))
                .groupByKey()
                .aggregate(
                        () -> initialCustomer,
                        (key, value, aggregate) -> value,
                        Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as("customer_enr_rekey")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(jsonSerde)
                );

        KTable<String, GenericRecord> callsCustomerJoin = callAggRekeyed
                .join(customerEnrRekeyed, (call, customer) -> {
                    final GenericRecord callCustomer = new GenericData.Record(schema);
                    callCustomer.put("id_telef_origen", call.get("ID_TELEF_ORIGEN").toString());
                    callCustomer.put("window_start_ts", call.get("WINDOW_START_TS").toString());
                    callCustomer.put("calls_count", call.get("CALLS_COUNT"));
                    callCustomer.put("max_duracion_origen", call.get("MAX_DURACION_ORIGEN"));
                    callCustomer.put("total_duracion_origen", call.get("TOTAL_DURACION_ORIGEN"));
                    callCustomer.put("avg_duracion_origen", call.get("AVG_DURACION_ORIGEN"));
                    callCustomer.put("doc_cliente", customer.get("DOC_CLIENTE").asText());
                    callCustomer.put("cliente_orange", customer.get("CLIENTE_ORANGE").asInt());
                    callCustomer.put("days_excliente", customer.get("DAYS_EXCLIENTE").asInt());
                    callCustomer.put("operador_actual", customer.get("OPERADOR_ACTUAL").asText());
                    callCustomer.put("riesgo", customer.get("RIESGO").asText());
                    return callCustomer;
                });

        callsCustomerJoin.toStream().to("CALLS_CLIENTES_ENR", Produced.with(stringSerde,valueAvroSerde));

        return builder.build();
    }

    public static void main(String[] args) throws IOException {
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

}