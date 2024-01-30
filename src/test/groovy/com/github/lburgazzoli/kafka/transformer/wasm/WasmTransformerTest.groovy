package com.github.lburgazzoli.kafka.transformer.wasm

import com.github.lburgazzoli.kafka.transformer.wasm.support.WasmTransformerTestSpec
import groovy.util.logging.Slf4j

import java.nio.charset.StandardCharsets

@Slf4j
class WasmTransformerTest extends WasmTransformerTestSpec {

    def 'simple function'() {
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
            recordOut.value() == "THE-VALUE".getBytes(StandardCharsets.UTF_8)
        cleanup:
            closeQuietly(t)
    }
}
