package com.github.lburgazzoli.kafka.transformer.wasm.support

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.header.ConnectHeaders
import org.apache.kafka.connect.header.Header
import org.apache.kafka.connect.source.SourceRecord
import org.testcontainers.redpanda.RedpandaContainer
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


    static KafkaProducer<byte[], byte[]> producer(RedpandaContainer container) {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, container.bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.name,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.name,
        ))
    }

    static KafkaConsumer<byte[], byte[]> consumer(RedpandaContainer container) {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, container.bootstrapServers,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.name,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.name,
                ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        ))
    }

}
