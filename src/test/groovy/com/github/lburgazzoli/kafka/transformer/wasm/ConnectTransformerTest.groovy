package com.github.lburgazzoli.kafka.transformer.wasm

import com.github.lburgazzoli.kafka.transformer.wasm.support.WasmTransformerTestSpec
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.redpanda.RedpandaContainer
import org.testcontainers.utility.DockerImageName
import spock.lang.Shared

import java.time.Duration

class ConnectTransformerTest extends WasmTransformerTestSpec {
    static final String IMAGE_NAME = "docker.redpanda.com/redpandadata/redpanda:v23.1.2"
    static final DockerImageName IMAGE = DockerImageName.parse(IMAGE_NAME)

    @Shared
    RedpandaContainer KAFKA = new RedpandaContainer(IMAGE)
            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("kafka.container")))


    def 'simple function'() {

        given:
            Producer<byte[], byte[]> producer = producer(KAFKA)
            Consumer<byte[], byte[]> consumer = consumer(KAFKA)

        when:
            producer.send(new ProducerRecord<>("test-topic" as byte[], "" as byte[]), (metadata, exception) -> {
                if (exception != null) {
                    log.wanr("Failed to send message: " + exception.getMessage());
                } else {
                    log.info("Message sent successfully! " +
                            "Topic: " + metadata.topic() +
                            ", Partition: " + metadata.partition() +
                            ", Offset: " + metadata.offset());
                }
            })
        then:
            def records = consumer.poll(Duration.ofMillis(100))
            records.size() == 1
        cleanup:
            closeQuietly(t)
    }


    KafkaProducer<byte[], byte[]> producer(RedpandaContainer container) {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, container.bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.name,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.name,
        ))
    }

    KafkaConsumer<byte[], byte[]> consumer(RedpandaContainer container) {
        return new KafkaConsumer<>()(Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, container.bootstrapServers,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.name,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.name,
            ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString()
        ))
    }
}
