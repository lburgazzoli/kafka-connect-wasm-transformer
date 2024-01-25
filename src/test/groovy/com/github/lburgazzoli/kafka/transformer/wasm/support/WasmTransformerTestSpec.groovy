package com.github.lburgazzoli.kafka.transformer.wasm.support


import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.header.ConnectHeaders
import org.apache.kafka.connect.header.Header
import org.apache.kafka.connect.source.SourceRecord
import spock.lang.Specification

class WasmTransformerTestSpec extends Specification {

    static void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (final IOException ignored) {
            }
        }
    }

    static SourceRecordBuilder sourceRecord() {
        return new SourceRecordBuilder()
    }

    static class SourceRecordBuilder {
        Map<String, ?> sourcePartition
        Map<String, ?> sourceOffset
        String topic
        Integer partition
        Schema keySchema
        Object key
        Schema valueSchema
        Object value
        Long timestamp
        Iterable< Header> headers

        SourceRecordBuilder withSourcePartition(Map<String, ?> sourcePartition) {
            this.sourcePartition = sourcePartition
            return this
        }

        SourceRecordBuilder withSourceOffset(Map<String, ?> sourceOffset) {
            this.sourceOffset = sourceOffset
            return this
        }

        SourceRecordBuilder withTopic(String topic) {
            this.topic = topic
            return this
        }

        SourceRecordBuilder withPartition(Integer partition) {
            this.partition = partition
            return this
        }

        SourceRecordBuilder withKeySchema(Schema keySchema) {
            this.keySchema = keySchema
            return this
        }

        SourceRecordBuilder withKey(Object key) {
            this.key = key
            return this
        }

        SourceRecordBuilder withValueSchema(Schema valueSchema) {
            this.valueSchema = valueSchema
            return this
        }

        SourceRecordBuilder withValue(Object value) {
            this.value = value
            return this
        }

        SourceRecordBuilder withTimestamp(Long timestamp) {
            this.timestamp = timestamp
            return this
        }

        SourceRecordBuilder withHeaders(Iterable<Header> headers) {
            this.headers = headers
            return this
        }

        SourceRecord build() {
            assert this.topic != null

            return new SourceRecord(
                    this.sourcePartition ?= Map.of(),
                    this.sourceOffset ?= Map.of(),
                    this.topic,
                    this.partition ?= 0,
                    this.keySchema,
                    this.key,
                    this.valueSchema,
                    this.value,
                    this.timestamp ?= 0,
                    this.headers ?= new ConnectHeaders())
        }
    }

}
