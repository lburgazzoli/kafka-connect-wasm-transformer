package com.github.lburgazzoli.kafka.support

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.slf4j.LoggerFactory
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.redpanda.RedpandaContainer
import org.testcontainers.utility.DockerImageName

class EmbeddedKafkaContainer extends RedpandaContainer {
    static final String IMAGE_NAME = "docker.redpanda.com/redpandadata/redpanda:v23.1.2"
    static final DockerImageName IMAGE = DockerImageName.parse(IMAGE_NAME)

    EmbeddedKafkaContainer() {
        super(IMAGE)
    }

    @Override
    protected void configure() {
        super.configure()
        super.withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(EmbeddedKafkaContainer.class)))
    }

    @Override
    void close() {
        super.configure()
        super.close()
    }

    KafkaProducer<byte[], byte[]> producer() {
        return producer(Map.of())
    }

    KafkaProducer<byte[], byte[]> producer(Map<String, String> overrides) {
        def params = new HashMap()
        params.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers)
        params.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.name)
        params.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.name)

        params.putAll(overrides)

        return new KafkaProducer<>(params)
    }

    KafkaConsumer<byte[], byte[]> consumer() {
        return consumer(Map.of())
    }

    KafkaConsumer<byte[], byte[]> consumer(Map<String, String> overrides) {
        def params = new HashMap()
        params.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers)
        params.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.name)
        params.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.name)
        params.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString())
        params.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

        params.putAll(overrides)

        return new KafkaConsumer<>(params)
    }
}
