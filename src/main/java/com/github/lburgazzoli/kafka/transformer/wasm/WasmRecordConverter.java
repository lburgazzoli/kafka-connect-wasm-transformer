package com.github.lburgazzoli.kafka.transformer.wasm;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;

public class WasmRecordConverter<R extends ConnectRecord<R>> {
    private final Converter keyConverter;
    private final Converter valueConverter;
    private final HeaderConverter headerConverter;

    public WasmRecordConverter(Converter keyConverter, Converter valueConverter, HeaderConverter headerConverter) {
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.headerConverter = headerConverter;
    }

    //
    // To Connect
    //

    public SchemaAndValue toConnectKey(R record, byte[] data) {
        return toConnectData(record, this.keyConverter, data);
    }

    public SchemaAndValue toConnectValue(R record, byte[] data) {
        return toConnectData(record, this.valueConverter, data);
    }

    public SchemaAndValue toConnectHeader(R record, String name, byte[] data) {
        return this.headerConverter.toConnectHeader(record.topic(), name, data);
    }

    private SchemaAndValue toConnectData(R record, Converter converter, byte[] data) {
        final RecordHeaders recordHeaders = new RecordHeaders();

        if (record.headers() != null) {
            // May not be needed but looks like the record headers may be required
            // by key/val converters
            for (Header header : record.headers()) {
                recordHeaders.add(
                    header.key(),
                    this.headerConverter.fromConnectHeader(record.topic(), header.key(), header.schema(), header.value()));
            }
        }

        return converter.toConnectData(record.topic(), recordHeaders, data);
    }

    //
    // From Connect
    //

    public byte[] fromConnectKey(R record) {
        return fromConnectData(record, this.keyConverter, new SchemaAndValue(record.keySchema(), record.key()));
    }

    public byte[] fromConnectValue(R record) {
        return fromConnectData(record, this.valueConverter, new SchemaAndValue(record.valueSchema(), record.value()));
    }

    public byte[] fromConnectHeader(R record, String name) {
        final Header header = record.headers().lastWithName(name);
        return this.headerConverter.fromConnectHeader(record.topic(), header.key(), header.schema(), header.value());
    }

    public byte[] fromConnectHeader(R record, Header header) {
        return this.headerConverter.fromConnectHeader(record.topic(), header.key(), header.schema(), header.value());
    }

    private byte[] fromConnectData(R record, Converter converter, SchemaAndValue schemaAndValue) {
        final String topic = record.topic();
        final RecordHeaders recordHeaders = new RecordHeaders();

        if (record.headers() != null) {
            // May not be needed but looks like the record headers may be required
            // by key/val converters
            for (Header header : record.headers()) {
                recordHeaders.add(
                    header.key(),
                    this.headerConverter.fromConnectHeader(topic, header.key(), header.schema(), header.value()));
            }
        }

        return converter.fromConnectData(topic, recordHeaders, schemaAndValue.schema(), schemaAndValue.value());
    }
}
