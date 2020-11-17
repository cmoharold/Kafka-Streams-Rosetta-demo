package com.harold.kafka.streams.calls.orange;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.harold.kafka.streams.schemas.avro.CallAggregate;
import com.harold.kafka.streams.schemas.avro.CallAggregateCust;
import com.harold.kafka.streams.schemas.avro.CustomerAggregate;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;

import static org.junit.Assert.assertEquals;

public class CallsEnrichedAppTest {

    private final static String TEST_CONFIG_FILE = "configuration/test.properties";
    private TopologyTestDriver testDriver;

    private SpecificAvroSerializer<CustomerAggregate> makeCustomerSerializer(Properties envProps) {
        SpecificAvroSerializer<CustomerAggregate> serializer = new SpecificAvroSerializer<>();

        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", envProps.getProperty("schema.registry.url"));
        serializer.configure(config, false);

        return serializer;
    }

    private SpecificAvroSerializer<CallAggregate> makeCallSerializer(Properties envProps) {
        SpecificAvroSerializer<CallAggregate> serializer = new SpecificAvroSerializer<>();

        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", envProps.getProperty("schema.registry.url"));
        serializer.configure(config, false);

        return serializer;
    }

    private SpecificAvroDeserializer<CallAggregateCust> makeCallCustomerDeserializer(Properties envProps) {
        SpecificAvroDeserializer<CallAggregateCust> deserializer = new SpecificAvroDeserializer<>();

        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", envProps.getProperty("schema.registry.url"));
        deserializer.configure(config, false);

        return deserializer;
    }

    private List<CallAggregateCust> readOutputTopic(TopologyTestDriver testDriver,
                                             String topic,
                                             Deserializer<String> keyDeserializer,
                                             SpecificAvroDeserializer<CallAggregateCust> makeCallCustomerDeserializer) {
        List<CallAggregateCust> results = new ArrayList<>();
        final TestOutputTopic<String, CallAggregateCust>
                testOutputTopic =
                testDriver.createOutputTopic(topic, keyDeserializer, makeCallCustomerDeserializer);
        testOutputTopic
                .readKeyValuesToList()
                .forEach(record -> {
                            if (record != null) {
                                results.add(record.value);
                            }
                        }
                );
        return results;
    }

    @Test
    public void testJoin() throws IOException {
        CallsEnrichedApp callsEnrichedApp = new CallsEnrichedApp();
        Properties envProps = callsEnrichedApp.loadEnvProperties(TEST_CONFIG_FILE);
        Properties streamProps = callsEnrichedApp.buildStreamsProperties(envProps);

        CallCustomerJoiner callCustomerJoiner = new CallCustomerJoiner();

        String tableTopic = envProps.getProperty("customer.topic.name");
        String streamTopic = envProps.getProperty("call.topic.name");
        String outputTopic = envProps.getProperty("call.customer.topic.name");

        Topology topology = callsEnrichedApp.buildTopology(envProps);
        testDriver = new TopologyTestDriver(topology, streamProps);

        Serializer<String> keySerializer = Serdes.String().serializer();
        SpecificAvroSerializer<CustomerAggregate> customerSerializer = makeCustomerSerializer(envProps);
        SpecificAvroSerializer<CallAggregate> callSerializer = makeCallSerializer(envProps);

        Deserializer<String> stringDeserializer = Serdes.String().deserializer();
        SpecificAvroDeserializer<CallAggregateCust> valueDeserializer = makeCallCustomerDeserializer(envProps);

        List<CustomerAggregate> customers = new ArrayList<>();
        customers.add(CustomerAggregate.newBuilder()
                .setTELEFONO("600000000")
                .setDOCCLIENTE("0000000R")
                .setCLIENTEORANGE(0)
                .setDAYSEXCLIENTE(0)
                .setOPERADORACTUAL("")
                .setRIESGO("")
                .build());

        List<CallAggregate> calls = new ArrayList<>();
        calls.add(CallAggregate.newBuilder()
                .setWINDOWSTARTTS(Long.valueOf(1232123412))
                .setIDTELEFORIGEN("600000000")
                .setCALLSCOUNT(5)
                .setMAXDURACIONORIGEN(3)
                .setTOTALDURACIONORIGEN(12)
                .setAVGDURACIONORIGEN(2)
                .build());

        List<CallAggregateCust> callsCustomers = new ArrayList<>();
        callsCustomers.add(CallAggregateCust.newBuilder()
                .setIdTelefOrigen("600000000")
                .setWindowStartTs(callCustomerJoiner.getReadableDate(calls.get(0).getWINDOWSTARTTS()))
                .setWindowEndTs(callCustomerJoiner.getReadableDatePlusOneHour(calls.get(0).getWINDOWSTARTTS()))
                .setCallsCount(5)
                .setMaxDuracionOrigen(3)
                .setTotalDuracionOrigen(12)
                .setAvgDuracionOrigen(2)
                .setDocCliente("0000000R")
                .setClienteOrange(0)
                .setDaysExcliente(0)
                .setOperadorActual("")
                .setRiesgo("")
                .build());

        final TestInputTopic<String, CustomerAggregate>
                customerTestInputTopic =
                testDriver.createInputTopic(tableTopic, keySerializer, customerSerializer);
        for (CustomerAggregate customer : customers) {
            customerTestInputTopic.pipeInput(String.valueOf(customer.getTELEFONO()), customer);
        }

        final TestInputTopic<String, CallAggregate>
                callTestInputTopic =
                testDriver.createInputTopic(streamTopic, keySerializer, callSerializer);
        for (CallAggregate call : calls) {
            callTestInputTopic.pipeInput(String.valueOf(call.getIDTELEFORIGEN()), call);
        }

        List<CallAggregateCust> actualOutput = readOutputTopic(testDriver, outputTopic, stringDeserializer, valueDeserializer);

        assertEquals(callsCustomers, actualOutput);
    }

    @After
    public void cleanup() {
        testDriver.close();
    }

}
