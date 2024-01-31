package com.github.lburgazzoli.kafka.transformer.wasm

import com.github.lburgazzoli.kafka.support.EmbeddedKafkaConnect
import com.github.lburgazzoli.kafka.support.EmbeddedKafkaContainer
import com.github.lburgazzoli.kafka.transformer.wasm.support.WasmTransformerTestSpec
import groovy.util.logging.Slf4j
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.file.FileStreamSourceConnector
import org.apache.kafka.connect.runtime.ConnectorConfig
import org.apache.kafka.connect.runtime.WorkerConfig
import org.apache.kafka.connect.runtime.isolation.PluginDiscoveryMode
import org.apache.kafka.connect.storage.StringConverter
import org.testcontainers.spock.Testcontainers
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

    @Shared
    EmbeddedKafkaContainer KAFKA = new EmbeddedKafkaContainer()

    @TempDir
    Path connectTmp

    def 'direct transformer (to_upper)'() {
        given:
            def t = new WasmTransformer()
            t.configure(Map.of(
                    WasmTransformer.WASM_MODULE_PATH, 'src/test/resources/functions.wasm',
                    WasmTransformer.WASM_FUNCTION_NAME, 'to_upper',
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

    def 'direct transformer (value_to_key)'() {
        given:
            def t = new WasmTransformer()
            t.configure(Map.of(
                    WasmTransformer.WASM_MODULE_PATH, 'src/test/resources/functions.wasm',
                    WasmTransformer.WASM_FUNCTION_NAME, 'value_to_key',
            ))

            def recordIn = sourceRecord()
                    .withTopic('foo')
                    .withKey('the-key'.getBytes(StandardCharsets.UTF_8))
                    .withValue('the-value'.getBytes(StandardCharsets.UTF_8))
                    .build()

        when:
            def recordOut = t.apply(recordIn)
        then:
            recordOut.key() == 'the-value'.getBytes(StandardCharsets.UTF_8)
        cleanup:
            closeQuietly(t)
    }

    def 'direct transformer (header_to_key)'() {
        given:
            def t = new WasmTransformer()
            t.configure(Map.of(
                    WasmTransformer.WASM_MODULE_PATH, 'src/test/resources/functions.wasm',
                    WasmTransformer.WASM_FUNCTION_NAME, 'header_to_key',
            ))

            def recordIn = sourceRecord()
                    .withTopic('foo')
                    .withKey('the-key'.getBytes(StandardCharsets.UTF_8))
                    .withValue('the-value'.getBytes(StandardCharsets.UTF_8))
                    .withHeader('the-key', Schema.BYTES_SCHEMA, 'my-key'.getBytes(StandardCharsets.UTF_8))
                    .build()

        when:
            def recordOut = t.apply(recordIn)
        then:
            recordOut.key() == 'my-key'.getBytes(StandardCharsets.UTF_8)
        cleanup:
            closeQuietly(t)
    }

    def 'direct transformer (copy_header)'() {
        given:
            def headerValue = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)

            def t = new WasmTransformer()
            t.configure(Map.of(
                    WasmTransformer.WASM_MODULE_PATH, 'src/test/resources/functions.wasm',
                    WasmTransformer.WASM_FUNCTION_NAME, 'copy_header',
            ))

            def recordIn = sourceRecord()
                    .withTopic('foo')
                    .withKey('the-key'.getBytes(StandardCharsets.UTF_8))
                    .withValue('the-value'.getBytes(StandardCharsets.UTF_8))
                    .withHeader('the-header-in', Schema.BYTES_SCHEMA, headerValue)
                    .build()

        when:
            def recordOut = t.apply(recordIn)
        then:
            recordOut.headers().lastWithName('the-header-out').value() == headerValue
        cleanup:
            closeQuietly(t)
    }

    def 'direct transformer (transform)'() {
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
            recordOut.key() == 'the-value'.getBytes(StandardCharsets.UTF_8)
        cleanup:
            closeQuietly(t)
    }

    def 'pipeline transformer (to_upper)'() {

        given:
            def inFile = connectTmp.resolve('in.txt')
            def topic = UUID.randomUUID().toString()
            def content = 'the-value'

            Producer<byte[], byte[]> producer = KAFKA.producer()
            Consumer<byte[], byte[]> consumer = KAFKA.consumer()

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
                    ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WasmTransformer.WASM_FUNCTION_NAME, 'to_upper',
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

            with(records.iterator().next()) {
                it.value() == content.toUpperCase(Locale.UK).getBytes(StandardCharsets.UTF_8)
            }

        cleanup:
            closeQuietly(producer)
            closeQuietly(consumer)
            closeQuietly(kc)
    }

    def 'pipeline transformer (value_to_key)'() {

        given:
            def inFile = connectTmp.resolve('in.txt')
            def topic = UUID.randomUUID().toString()
            def content = UUID.randomUUID().toString()

            Producer<byte[], byte[]> producer = KAFKA.producer()
            Consumer<byte[], byte[]> consumer = KAFKA.consumer()

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
                    ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WasmTransformer.WASM_FUNCTION_NAME, 'value_to_key',
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

            with(records.iterator().next()) {
                it.key() == content.getBytes(StandardCharsets.UTF_8)
                it.value() == content.getBytes(StandardCharsets.UTF_8)
            }

        cleanup:
            closeQuietly(producer)
            closeQuietly(consumer)
            closeQuietly(kc)
    }

    def 'pipeline transformer (transform)'() {

        given:
            def inFile = connectTmp.resolve('in.txt')
            def topic = UUID.randomUUID().toString()
            def content = 'the-value'

            Producer<byte[], byte[]> producer = KAFKA.producer()
            Consumer<byte[], byte[]> consumer = KAFKA.consumer()

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

            with(records.iterator().next()) {
                it.key() == content.getBytes(StandardCharsets.UTF_8)
                it.value() == content.toUpperCase(Locale.UK).getBytes(StandardCharsets.UTF_8)
            }

        cleanup:
            closeQuietly(producer)
            closeQuietly(consumer)
            closeQuietly(kc)
    }
}
