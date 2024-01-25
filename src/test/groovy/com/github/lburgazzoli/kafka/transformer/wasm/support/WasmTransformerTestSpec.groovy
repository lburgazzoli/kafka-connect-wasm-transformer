package com.github.lburgazzoli.kafka.transformer.wasm.support

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

}
