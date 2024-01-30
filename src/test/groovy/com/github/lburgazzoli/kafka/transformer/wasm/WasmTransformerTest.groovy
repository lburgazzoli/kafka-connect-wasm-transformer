package com.github.lburgazzoli.kafka.transformer.wasm

import com.github.lburgazzoli.kafka.support.EmbeddedKafkaConnect
import com.github.lburgazzoli.kafka.transformer.wasm.support.WasmTransformerTestSpec
import groovy.util.logging.Slf4j
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.connect.file.FileStreamSourceConnector
import org.apache.kafka.connect.runtime.ConnectorConfig
import org.apache.kafka.connect.runtime.WorkerConfig
import org.apache.kafka.connect.runtime.WorkerConfigTransformer
import org.apache.kafka.connect.runtime.isolation.PluginDiscoveryMode
import org.apache.kafka.connect.storage.StringConverter
import org.slf4j.LoggerFactory
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.redpanda.RedpandaContainer
import org.testcontainers.spock.Testcontainers
import org.testcontainers.utility.DockerImageName
import spock.lang.Shared
import spock.lang.TempDir

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Duration

@Slf4j
@Testcontainers
class WasmTransformerTest extends WasmTransformerTestSpec {
    static final String IMAGE_NAME = "docker.redpanda.com/redpandadata/redpanda:v23.1.2"
    static final DockerImageName IMAGE = DockerImageName.parse(IMAGE_NAME)

    @Shared
    RedpandaContainer KAFKA = new RedpandaContainer(IMAGE)
            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("kafka.container")))

    @TempDir
    Path connectTmp

    def 'direct transformer'() {
        given:
            def t = new WasmTransformer()
            t.configure(Map.of(
                    WasmTransformer.WASM_MODULE_PATH, 'src/test/resources/functions.wasm',
                    WasmTransformer.WASM_FUNCTION_NAME, 'transform',
            ))

            def recordIn = sourceRecord()
                    .withTopic('foo')
                    .withKey('the-key'.getBytes(StandardCharsets.UTF_8))
                    .withValue('the-value'.getBytes(StandardCharsets.UTF_8))
                    .build()

        when:
            def recordOut = t.apply(recordIn)
        then:
            recordOut.value() == 'THE-VALUE'.getBytes(StandardCharsets.UTF_8)
            cleanup:
            closeQuietly(t)
    }

    def 'pipeline transformer'() {

        given:
            def inFile = connectTmp.resolve('in.txt')
            def topic = UUID.randomUUID().toString()
            def content = 'the-value'

            Producer<byte[], byte[]> producer = producer(KAFKA)
            Consumer<byte[], byte[]> consumer = consumer(KAFKA)

            def kc = new EmbeddedKafkaConnect()
            kc.setProperty(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.bootstrapServers)
            kc.setProperty(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.name)
            kc.setProperty(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.name)
            kc.setProperty(WorkerConfig.PLUGIN_DISCOVERY_CONFIG, PluginDiscoveryMode.SERVICE_LOAD.name())

            kc.setConnectorDefinition('file-source', FileStreamSourceConnector.class, Map.of(
                    FileStreamSourceConnector.FILE_CONFIG, inFile.toString(),
                    FileStreamSourceConnector.TOPIC_CONFIG, topic,
                    ConnectorConfig.TRANSFORMS_CONFIG, 'wasm',
                    ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.type', WasmTransformer.class.name,
                    ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WasmTransformer.WASM_MODULE_PATH, 'src/test/resources/functions.wasm',
                    ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WasmTransformer.WASM_FUNCTION_NAME, 'transform',
                    ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WasmTransformer.KEY_CONVERTER, StringConverter.class.name,
                    ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WasmTransformer.VALUE_CONVERTER, StringConverter.class.name,
                    ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WasmTransformer.HEADER_CONVERTER, StringConverter.class.name,
            ))

            kc.start()

            // subscribe to the topic
            consumer.subscribe(Collections.singletonList(topic))

        when:
            // write something in the input file
            Files.writeString(inFile, content + '\n', StandardOpenOption.APPEND, StandardOpenOption.CREATE)

        then:
            def records = consumer.poll(Duration.ofSeconds(5))
            records.size() == 1
            records.iterator().next().value() == 'THE-VALUE'.getBytes(StandardCharsets.UTF_8)

        cleanup:
            closeQuietly(producer)
            closeQuietly(consumer)
            closeQuietly(kc)
    }
}
