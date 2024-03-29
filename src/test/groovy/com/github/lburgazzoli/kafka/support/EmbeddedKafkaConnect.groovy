package com.github.lburgazzoli.kafka.support

import groovy.util.logging.Slf4j
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.utils.Time
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy
import org.apache.kafka.connect.runtime.ConnectorConfig
import org.apache.kafka.connect.runtime.Herder
import org.apache.kafka.connect.runtime.Worker
import org.apache.kafka.connect.runtime.WorkerConfig
import org.apache.kafka.connect.runtime.isolation.Plugins
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore
import org.apache.kafka.connect.util.Callback
import org.apache.kafka.connect.util.FutureCallback

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

@Slf4j
class EmbeddedKafkaConnect implements AutoCloseable {
    private Map<String,String> properties
    private Map<String, EmbeddedConnector> connectors
    private AtomicBoolean running
    private ExecutorService executor

    private Herder herder

    EmbeddedKafkaConnect() {
        this.properties = new HashMap<>()
        this.connectors = new HashMap<>()
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
        this.connectors.put(name, new EmbeddedConnector(
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
        log.info("Starting KafkaConnect")

        try {
            log.info("Worker initializing ...")
            long initStart = Time.SYSTEM.hiResClockMs()

            Map<String, String> workerProps = Map.copyOf(properties)
            log.info("props {}", workerProps)

            log.info("Scanning for plugin classes. This might take a moment ...")
            Plugins plugins = new Plugins(workerProps)
            plugins.compareAndSwapWithDelegatingLoader()

            WorkerConfig config = new EmbeddedConfig(workerProps)

            String kafkaClusterId = config.kafkaClusterId()
            log.debug("Kafka cluster ID: {}", kafkaClusterId)

            ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy = plugins.newPlugin(
                    config.getString(WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG),
                    config,
                    ConnectorClientConfigOverridePolicy.class)

            Worker worker = new Worker(
                    "embedded",
                    Time.SYSTEM,
                    plugins,
                    config,
                    new MemoryOffsetBackingStore() {
                        @Override
                        Set<Map<String, Object>> connectorPartitions(String connectorName) {
                            return Collections.emptySet()
                        }
                    },
                    connectorClientConfigOverridePolicy)

            herder = new StandaloneHerder(worker, kafkaClusterId, connectorClientConfigOverridePolicy)
            log.info("Worker initialization took {}ms", Time.SYSTEM.hiResClockMs() - initStart)

            try {
                herder.start()

                for (def cd : connectors.entrySet()) {
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
        log.info("Stop KafkaConnect")
        if (this.herder != null) {
            herder.stop()
        }
    }

    private static class EmbeddedConnector {
        String name
        String type
        Map<String, String> config;
    }

    private static class EmbeddedConfig extends WorkerConfig {
        private static final ConfigDef CONFIG

        static {
            CONFIG = baseConfigDef()
        }

        EmbeddedConfig(Map<String, String> props) {
            super(CONFIG, props)
        }
    }
}
