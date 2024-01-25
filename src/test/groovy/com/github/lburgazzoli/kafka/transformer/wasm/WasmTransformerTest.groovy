package com.github.lburgazzoli.kafka.transformer.wasm

import com.github.lburgazzoli.kafka.transformer.wasm.support.WasmTransformerTestSpec
import groovy.util.logging.Slf4j
import org.apache.kafka.connect.header.ConnectHeaders
import org.apache.kafka.connect.source.SourceRecord

import java.nio.charset.StandardCharsets

@Slf4j
class WasmTransformerTest extends WasmTransformerTestSpec{

    def 'simple test'() {
        given:
            def t = new WasmTransformer()
            t.configure(Map.of(
                WasmTransformer.WASM_MODULE_PATH, 'src/test/resources/functions.wasm',
                WasmTransformer.WASM_FUNCTION_NAME, 'transform',
            ))

            def recordIn = sourceRecord(
                    'foo',
                    'the-key'.getBytes(StandardCharsets.UTF_8),
                    'the-value'.getBytes(StandardCharsets.UTF_8))

        when:
            def recordOut = t.apply(recordIn)
        then:
            recordOut.value() == "THE-VALUE"
        cleanup:
            closeQuietly(t)
    }

    private static SourceRecord sourceRecord(String topic, byte[] key,  byte[] value) {
        return new SourceRecord(
                Map.of("foo", "bar"),
                Map.of("baz", "quxx"),
                topic,
                0,
                null,
                key,
                null,
                value,
                0L,
                new ConnectHeaders())
    }
}
