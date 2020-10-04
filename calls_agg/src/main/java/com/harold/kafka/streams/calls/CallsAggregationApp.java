package com.harold.kafka.streams.calls;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
//import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.kstream.*;

public class CallsAggregationApp {

    private static String clientId = "calls-orange";
    private static String groupId = "rosetta";
    private static String endpoints = "localhost:9092";
    private static String table_calls = "CALLS";
    private static String autoOffsetResetPolicy = "earliest";
    private static String streamsNumOfThreads = "3";
    private static String schemaRegistryUrl = "http://localhost:8081";
    private static String keySerde = "org.apache.kafka.common.serialization.Serdes$StringSerde";
    private static String valueSerde = GenericAvroSerde.class.getCanonicalName();
    private static String deserializationExceptionHandler = LogAndContinueExceptionHandler.class.getCanonicalName();

    public Topology createTopology() throws IOException {

        final Serde<String> stringSerde = Serdes.String();
        // value avro Serde
        final Serde<GenericRecord> valueAvroSerde = new GenericAvroSerde();
        valueAvroSerde.configure(getSerdeProperties(), false);

        final InputStream
                callsAggregateSchema =
                CallsAggregationApp.class.getClassLoader()
                        .getResourceAsStream("callagg.avsc");
        final Schema schema = new Schema.Parser().parse(callsAggregateSchema);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, GenericRecord> calls = builder.stream(table_calls, Consumed.with(stringSerde, valueAvroSerde));

        KTable<Windowed<String>, GenericRecord> callAggRekeyed = calls
                .selectKey((key, value) -> value.get("id_telef_origen").toString())
                //.groupBy((id, call) -> call.get("id_telef_origen").toString(), Grouped.with(Serdes.String(), valueAvroSerde))
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofHours(1)))
                .aggregate(
                        () -> {
                            final GenericRecord initialCalls = new GenericData.Record(schema);
                            initialCalls.put("window_start_ts", "1970-01-01 00:00:00");
                            initialCalls.put("id_telef_origen", "0");
                            initialCalls.put("calls_count", 0);
                            initialCalls.put("max_duracion_origen", 0);
                            initialCalls.put("total_duracion_origen", 0);
                            initialCalls.put("avg_duracion_origen", 0);
                            return initialCalls;
                        },
                        (key, value, aggregate) -> {
                            final GenericRecord aggCalls = new GenericData.Record(schema);
                            aggCalls.put("window_start_ts", "");
                            aggCalls.put("id_telef_origen", value.get("id_telef_origen").toString());
                            aggCalls.put("calls_count", 0);
                            aggCalls.put("max_duracion_origen", Math.max((long) value.get("duracion_origen"),(long) value.get("duracion_origen")));
                            aggCalls.put("total_duracion_origen", (long) value.get("duracion_origen") + (long) value.get("duracion_origen"));
                            aggCalls.put("avg_duracion_origen", 0);
                            return aggCalls;
                        });

        KStream<String, GenericRecord> callWithAggregations = callAggRekeyed
                .toStream((stringWindowed, genericRecord) -> stringWindowed.key())
                .filter((k, v) -> v != null)
                .selectKey((id, callValue) -> callValue.get("id_telef_origen").toString());

        callWithAggregations.to("CALLS_CLIENTES_ENR_NEW", Produced.with(stringSerde,valueAvroSerde));

        return builder.build();
    }

    public static void main(String[] args) throws IOException {
        Properties config = new Properties();

        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetPolicy);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, clientId);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, keySerde);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerde);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, endpoints);
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, streamsNumOfThreads);
        config.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, deserializationExceptionHandler);
        config.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        CallsAggregationApp callsEnrichedApp = new CallsAggregationApp();

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