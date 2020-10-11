package com.harold.kafka.streams.calls.orange;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.*;

public class CallsEnrichedAppTest {


    private TopologyTestDriver testDriver;
    private TestInputTopic<String, GenericRecord> inputTopicCalls;
    private TestInputTopic<String, JsonNode> inputTopicCustomers;
    private TestOutputTopic<String, GenericRecord> outputTopic;

    private StringSerializer stringSerializer = new StringSerializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();

    private static final String SCHEMA_REGISTRY_SCOPE = CallsEnrichedAppTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;

    final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
    final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();

    final Serde<GenericRecord> genericAvroSerde = new GenericAvroSerde();

    private Schema schemaCalls;

    private SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope(SCHEMA_REGISTRY_SCOPE);

    @Before
    public void setUpTopologyTestDriver() throws IOException, RestClientException {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class.getCanonicalName());
        config.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

        CallsEnrichedApp callsEnrichedApp = new CallsEnrichedApp();

        Topology topology = callsEnrichedApp.createTopology();
        testDriver = new TopologyTestDriver(topology, config);

        final InputStream
                callsAggregateSchema =
                CallsEnrichedAppTest.class.getClassLoader()
                        .getResourceAsStream("callagg.avsc");
        schemaCalls = new Schema.Parser().parse(callsAggregateSchema);

        schemaRegistryClient.register(callsEnrichedApp.table_calls + "-value", schemaCalls);

        genericAvroSerde.configure(
                Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL),
                /*isKey*/ false);

        inputTopicCalls =
                testDriver.createInputTopic(callsEnrichedApp.table_calls, stringSerializer, genericAvroSerde.serializer());

        inputTopicCustomers =
                testDriver.createInputTopic(callsEnrichedApp.table_clientes, stringSerializer, jsonSerializer);

        outputTopic =
                testDriver.createOutputTopic(callsEnrichedApp.stream_call_clientes, stringDeserializer, genericAvroSerde.deserializer());
    }

    @After
    public void closeTestDriver(){
        if (testDriver != null) {
            testDriver.close();
        }
        MockSchemaRegistry.dropScope(SCHEMA_REGISTRY_SCOPE);
    }

    public GenericRecord getInputRecordCall(String telefono) {
        GenericRecord callRecord = new GenericData.Record(schemaCalls);
        callRecord.put("WINDOW_START_TS", Long.valueOf(1232123412));
        callRecord.put("ID_TELEF_ORIGEN", telefono);
        callRecord.put("CALLS_COUNT", 5);
        callRecord.put("MAX_DURACION_ORIGEN", 3);
        callRecord.put("TOTAL_DURACION_ORIGEN", 12);
        callRecord.put("AVG_DURACION_ORIGEN", 2);
        return callRecord;
    }

    public JsonNode getInputRecordCustomer(String telefono) {
        ObjectNode customerRecord = JsonNodeFactory.instance.objectNode();
        customerRecord.put("TELEFONO", telefono);
        customerRecord.put("DOC_CLIENTE", "0000000R");
        customerRecord.put("CLIENTE_ORANGE", 0);
        customerRecord.put("DAYS_EXCLIENTE", 0);
        customerRecord.put("OPERADOR_ACTUAL", "");
        customerRecord.put("RIESGO", "");
        return customerRecord;
    }

    @Test
    public void makeSureKeyIsCorrect() {
        assertTrue(outputTopic.isEmpty());
        String telefono = "600000000";
        inputTopicCustomers.pipeInput(telefono,getInputRecordCustomer(telefono));
        inputTopicCalls.pipeInput(telefono,getInputRecordCall(telefono));
        assertEquals(outputTopic.readKeyValue().key,telefono);
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void makeSureLeftJoinCorrectSomeDataCustomer(){
        assertTrue(outputTopic.isEmpty());
        String telefono = "600000000";
        inputTopicCustomers.pipeInput(telefono,getInputRecordCustomer(telefono));
        inputTopicCalls.pipeInput(telefono,getInputRecordCall(telefono));
        assertEquals(outputTopic.readValue().get("DOC_CLIENTE"),getInputRecordCustomer(telefono).get("doc_cliente"));
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void makeSureLeftJoinCorrectSomeDataCall(){
        assertTrue(outputTopic.isEmpty());
        String telefono = "600000000";
        inputTopicCustomers.pipeInput(telefono,getInputRecordCustomer(telefono));
        inputTopicCalls.pipeInput(telefono,getInputRecordCall(telefono));
        assertEquals(outputTopic.readValue().get("calls_count"),getInputRecordCall(telefono).get("CALLS_COUNT"));
        assertTrue(outputTopic.isEmpty());
    }
}
