package com.harold.kafka.streams.calls.orange;

import org.junit.Test;

import com.harold.kafka.streams.schemas.avro.CallAggregate;
import com.harold.kafka.streams.schemas.avro.CallAggregateCust;
import com.harold.kafka.streams.schemas.avro.CustomerAggregate;

import static org.junit.Assert.assertEquals;

public class CallCustomerJoinerTest {
    @Test
    public void apply() {

        CallCustomerJoiner joiner = new CallCustomerJoiner();
        CallAggregateCust actualCallCustomer;

        CustomerAggregate customer = CustomerAggregate.newBuilder()
                .setTELEFONO("600000000")
                .setDOCCLIENTE("0000000R")
                .setCLIENTEORANGE(0)
                .setDAYSEXCLIENTE(0)
                .setOPERADORACTUAL("")
                .setRIESGO("")
                .build();
        CallAggregate call = CallAggregate.newBuilder()
                .setWINDOWSTARTTS(Long.valueOf(1232123412))
                .setIDTELEFORIGEN("600000000")
                .setCALLSCOUNT(5)
                .setMAXDURACIONORIGEN(3)
                .setTOTALDURACIONORIGEN(12)
                .setAVGDURACIONORIGEN(2)
                .build();
        CallAggregateCust expectedCallCustomer = CallAggregateCust.newBuilder()
                .setIdTelefOrigen("600000000")
                .setWindowStartTs(joiner.getReadableDate(call.getWINDOWSTARTTS()))
                .setWindowEndTs(joiner.getReadableDatePlusOneHour(call.getWINDOWSTARTTS()))
                .setCallsCount(5)
                .setMaxDuracionOrigen(3)
                .setTotalDuracionOrigen(12)
                .setAvgDuracionOrigen(2)
                .setDocCliente("0000000R")
                .setClienteOrange(0)
                .setDaysExcliente(0)
                .setOperadorActual("")
                .setRiesgo("")
                .build();

        actualCallCustomer = joiner.apply(call, customer);

        assertEquals(actualCallCustomer, expectedCallCustomer);
    }
}
