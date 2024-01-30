package com.github.lburgazzoli.kafka.transformer.wasm.support

import groovy.util.logging.Slf4j
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy
import org.apache.kafka.connect.runtime.Herder
import org.apache.kafka.connect.runtime.Worker
import org.apache.kafka.connect.runtime.isolation.Plugins
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore
import org.apache.kafka.connect.util.Callback
import org.apache.kafka.connect.util.FutureCallback

@Slf4j
class ConnectMain {
    public void run() {
        log.info("Start ConnectStandalone")

        try {
            log.info("Kafka Connect standalone worker initializing ...")
            long initStart = time.hiResClockMs()

            Map<String, String> workerProps = qnectConfig.worker.properties
            log.info("props {}", workerProps)

            log.info("Scanning for plugin classes. This might take a moment ...")
            Plugins plugins = new Plugins(workerProps)
            plugins.compareAndSwapWithDelegatingLoader()
            StandaloneConfig config = new StandaloneConfig(workerProps)

            String kafkaClusterId = ConnectUtils.lookupKafkaClusterId(config)
            log.debug("Kafka cluster ID: {}", kafkaClusterId)

            ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy = plugins.newPlugin(
                    config.getString(WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG),
                    config,
                    ConnectorClientConfigOverridePolicy.class)

            Worker worker = new Worker(
                    qnectConfig.worker.name,
                    time,
                    plugins,
                    config,
                    new MemoryOffsetBackingStore() {
                        @Override
                        Set<Map<String, Object>> connectorPartitions(String connectorName) {
                            return Set.of()
                        }
                    },
                    connectorClientConfigOverridePolicy)

            herder = new StandaloneHerder(worker, kafkaClusterId, connectorClientConfigOverridePolicy)
            log.info("Kafka Connect standalone worker initialization took {}ms", time.hiResClockMs() - initStart)

            try {
                herder.start()
                for (def cc : qnectConfig.connectors.entrySet()) {
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

                    Map<String, String> cp = new HashMap<>(cc.getValue().properties)
                    cp.put(ConnectorConfig.NAME_CONFIG, cc.getKey())
                    cp.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, cc.getValue().type)

                    herder.putConnectorConfig(
                            cc.getKey(),
                            cp,
                            false,
                            cb)

                    cb.get()
                }
            } catch (Throwable t) {
                log.error("Stopping after connector error", t)
                stop()
                Exit.exit(3)
            }
        } catch (Throwable t) {
            log.error("Stopping due to error", t)
            Exit.exit(2)
        }
    }

    void stop() {
        log.info("Stop ConnectStandalone")
        if (this.herder != null) {
            herder.stop()
        }
    }
}
