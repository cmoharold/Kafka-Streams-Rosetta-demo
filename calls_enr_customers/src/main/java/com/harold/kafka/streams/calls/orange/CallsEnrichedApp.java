package com.harold.kafka.streams.calls.orange;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import com.harold.kafka.streams.schemas.avro.CallAggregate;
import com.harold.kafka.streams.schemas.avro.CallAggregateCust;
import com.harold.kafka.streams.schemas.avro.CustomerAggregate;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class CallsEnrichedApp {

    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));

        return props;
    }

    public Topology buildTopology(Properties envProps) {
        final StreamsBuilder builder = new StreamsBuilder();
        final String customerTopic = envProps.getProperty("customer.topic.name");
        final String rekeyedCustomerTopic = envProps.getProperty("rekeyed.customer.topic.name");
        final String callTopic = envProps.getProperty("call.topic.name");
        final String callCustomersTopic = envProps.getProperty("call.customer.topic.name");
        final CallCustomerJoiner joiner = new CallCustomerJoiner();

        KStream<String, CustomerAggregate> customerStream = builder.<String, CustomerAggregate>stream(customerTopic)
                .map((key, customer) -> new KeyValue<>(String.valueOf(customer.getTELEFONO()), customer));

        customerStream.to(rekeyedCustomerTopic);

        KTable<String, CustomerAggregate> customers = builder.table(rekeyedCustomerTopic);

        KStream<String, CallAggregate> calls = builder.<String, CallAggregate>stream(callTopic)
                .map((key, call) -> new KeyValue<>(String.valueOf(call.getIDTELEFORIGEN()), call));

        KStream<String, CallAggregateCust> callCustomer = calls.leftJoin(customers, joiner);

        callCustomer.to(callCustomersTopic, Produced.with(Serdes.String(), callCustomerAvroSerde(envProps)));

        return builder.build();
    }

    private SpecificAvroSerde<CallAggregateCust> callCustomerAvroSerde(Properties envProps) {
        SpecificAvroSerde<CallAggregateCust> customerAvroSerde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG,
                envProps.getProperty("schema.registry.url"));

        customerAvroSerde.configure(serdeConfig, false);
        return customerAvroSerde;
    }

    public void createTopics(Properties envProps) {
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", envProps.getProperty("bootstrap.servers"));
        AdminClient client = AdminClient.create(config);

        List<NewTopic> topics = new ArrayList<>();

        topics.add(new NewTopic(
                envProps.getProperty("customer.topic.name"),
                Integer.parseInt(envProps.getProperty("customer.topic.partitions")),
                Short.parseShort(envProps.getProperty("customer.topic.replication.factor"))));

        topics.add(new NewTopic(
                envProps.getProperty("rekeyed.customer.topic.name"),
                Integer.parseInt(envProps.getProperty("rekeyed.customer.topic.partitions")),
                Short.parseShort(envProps.getProperty("rekeyed.customer.topic.replication.factor"))));

        topics.add(new NewTopic(
                envProps.getProperty("call.topic.name"),
                Integer.parseInt(envProps.getProperty("call.topic.partitions")),
                Short.parseShort(envProps.getProperty("call.topic.replication.factor"))));

        topics.add(new NewTopic(
                envProps.getProperty("call.customer.topic.name"),
                Integer.parseInt(envProps.getProperty("call.customer.topic.partitions")),
                Short.parseShort(envProps.getProperty("call.customer.topic.replication.factor"))));

        client.createTopics(topics);
        client.close();
    }

    public Properties loadEnvProperties(String fileName) throws IOException {
        Properties envProps = new Properties();
        FileInputStream input = new FileInputStream(fileName);
        envProps.load(input);
        input.close();

        return envProps;
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: the path to an environment configuration file.");
        }

        CallsEnrichedApp callsEnrichedApp = new CallsEnrichedApp();
        Properties envProps = callsEnrichedApp.loadEnvProperties(args[0]);
        Properties streamProps = callsEnrichedApp.buildStreamsProperties(envProps);
        Topology topology = callsEnrichedApp.buildTopology(envProps);

        callsEnrichedApp.createTopics(envProps);

        final KafkaStreams streams = new KafkaStreams(topology, streamProps);
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
