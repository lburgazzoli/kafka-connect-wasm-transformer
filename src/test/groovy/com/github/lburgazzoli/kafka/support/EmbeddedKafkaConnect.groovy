package com.github.lburgazzoli.kafka.support

import groovy.util.logging.Slf4j
import org.apache.kafka.common.utils.Time
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy
import org.apache.kafka.connect.json.JsonConverter
import org.apache.kafka.connect.json.JsonConverterConfig
import org.apache.kafka.connect.runtime.ConnectorConfig
import org.apache.kafka.connect.runtime.Herder
import org.apache.kafka.connect.runtime.Worker
import org.apache.kafka.connect.runtime.WorkerConfig
import org.apache.kafka.connect.runtime.isolation.Plugins
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder
import org.apache.kafka.connect.storage.FileOffsetBackingStore
import org.apache.kafka.connect.storage.OffsetBackingStore
import org.apache.kafka.connect.util.Callback
import org.apache.kafka.connect.util.FutureCallback

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

@Slf4j
class EmbeddedKafkaConnect implements AutoCloseable {
    private Map<String,String> properties
    private Map<String, ConnectorDefinition> connectorDefinitions
    private AtomicBoolean running
    private ExecutorService executor

    private Herder herder

    EmbeddedKafkaConnect() {
        this.properties = new HashMap<>()
        this.connectorDefinitions = new HashMap<>()
        this.running = new AtomicBoolean(false)
        this.executor = Executors.newSingleThreadExecutor()
    }

    void setProperty(String key, String value) {
        this.properties.put(key, value)
    }

    void setProperties(Map<String,String> properties) {
        this.properties.putAll(properties)
    }

    void setConnectorDefinition(String name, Class<?> type, Map<String, String> properties) {
        this.connectorDefinitions.put(name, new ConnectorDefinition(
                name: name,
                type: type.name,
                config: Map.copyOf(properties)
        ))
    }

    void start() {
        if (!this.running.compareAndExchange(false, true)) {
            this.executor.submit(this::doRun)
        }
    }

    @Override
    void close() {
        if (this.running.compareAndExchange(true, false)) {
            doStop()
        }
    }

    private void doRun() {
        log.info("Start ConnectStandalone")

        try {
            log.info("Kafka Connect standalone worker initializing ...")
            long initStart = Time.SYSTEM.hiResClockMs()

            Map<String, String> workerProps = Map.copyOf(properties)
            log.info("props {}", workerProps)

            log.info("Scanning for plugin classes. This might take a moment ...")
            Plugins plugins = new Plugins(workerProps)
            plugins.compareAndSwapWithDelegatingLoader()

            StandaloneConfig config = new StandaloneConfig(workerProps)

            String kafkaClusterId = config.kafkaClusterId()
            log.debug("Kafka cluster ID: {}", kafkaClusterId)

            ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy = plugins.newPlugin(
                    config.getString(WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG),
                    config,
                    ConnectorClientConfigOverridePolicy.class)


            OffsetBackingStore obs = new FileOffsetBackingStore(
                    plugins.newInternalConverter(
                            true,
                            JsonConverter.class.getName(),
                            Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false")))

            obs.configure(config)

            Worker worker = new Worker(
                    "standalone",
                    Time.SYSTEM,
                    plugins,
                    config,
                    obs,
                    connectorClientConfigOverridePolicy)

            herder = new StandaloneHerder(worker, kafkaClusterId, connectorClientConfigOverridePolicy)
            log.info("Kafka Connect standalone worker initialization took {}ms", Time.SYSTEM.hiResClockMs() - initStart)

            try {
                herder.start()

                for (def cd : connectorDefinitions.entrySet()) {
                    var cb = new FutureCallback<>(new Callback<Herder.Created<ConnectorInfo>>() {
                        @Override
                        void onCompletion(Throwable error, Herder.Created<ConnectorInfo> info) {
                            if (error != null) {
                                log.error("Failed to create job")
                            } else {
                                log.info("Created connector {}", info.result().name())
                            }
                        }
                    })

                    Map<String, String> cp = new HashMap<>(cd.value.config)
                    cp.put(ConnectorConfig.NAME_CONFIG, cd.key)
                    cp.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, cd.value.type)

                    herder.putConnectorConfig(
                            cd.key,
                            cp,
                            false,
                            cb)

                    cb.get()
                }
            } catch (Throwable t) {
                log.error("Stopping after connector error", t)
                close()
                throw new RuntimeException(t)
            }
        } catch (Throwable t) {
            log.error("Stopping due to error", t)
            close()
            throw new RuntimeException(t)
        }
    }

    private void doStop() {
        log.info("Stop ConnectStandalone")
        if (this.herder != null) {
            herder.stop()
        }
    }

    private static class ConnectorDefinition {
        String name
        String type
        Map<String, String> config;
    }

}
