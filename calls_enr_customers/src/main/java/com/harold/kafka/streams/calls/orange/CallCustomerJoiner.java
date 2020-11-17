package com.harold.kafka.streams.calls.orange;

import org.apache.kafka.streams.kstream.ValueJoiner;

import com.harold.kafka.streams.schemas.avro.CallAggregate;
import com.harold.kafka.streams.schemas.avro.CallAggregateCust;
import com.harold.kafka.streams.schemas.avro.CustomerAggregate;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.lang3.time.DateUtils;

public class CallCustomerJoiner implements ValueJoiner<CallAggregate, CustomerAggregate, CallAggregateCust> {

    public CallAggregateCust apply(CallAggregate call, CustomerAggregate customer) {
        return CallAggregateCust.newBuilder()
                .setIdTelefOrigen(call.getIDTELEFORIGEN())
                .setWindowStartTs(getReadableDate(call.getWINDOWSTARTTS()))
                .setWindowEndTs(getReadableDatePlusOneHour(call.getWINDOWSTARTTS()))
                .setCallsCount(call.getCALLSCOUNT())
                .setMaxDuracionOrigen(call.getMAXDURACIONORIGEN())
                .setTotalDuracionOrigen(call.getTOTALDURACIONORIGEN())
                .setAvgDuracionOrigen(call.getAVGDURACIONORIGEN())
                .setDocCliente(customer==null ? "Unknown" : customer.getDOCCLIENTE())
                .setClienteOrange(customer==null ? 0 : customer.getCLIENTEORANGE())
                .setDaysExcliente(customer==null ? 0 : customer.getDAYSEXCLIENTE())
                .setOperadorActual(customer==null ? "Unknown" : customer.getOPERADORACTUAL())
                .setRiesgo(customer==null ? "Unknown" : customer.getRIESGO())
                .build();
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