package com.github.lburgazzoli.kafka.transformer.wasm;

import java.io.File;
import java.util.Map;

import com.dylibso.chicory.runtime.Module;
import com.dylibso.chicory.runtime.exceptions.WASMMachineException;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WasmTransformer<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {
    private static final Logger LOGGER = LoggerFactory.getLogger(WasmTransformer.class);

    public static final String KEY_CONVERTER = "key.converter";
    public static final String VALUE_CONVERTER = "value.converter";
    public static final String HEADER_CONVERTER = "header.converter";
    public static final String WASM_MODULE_PATH = "module.path";
    public static final String WASM_FUNCTION_NAME = "function.name";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(WASM_MODULE_PATH,
            ConfigDef.Type.STRING,
            null,
            new ConfigDef.NonEmptyString(),
            ConfigDef.Importance.HIGH,
            "The Wasm module path.")
        .define(WASM_FUNCTION_NAME,
            ConfigDef.Type.STRING,
            null,
            new ConfigDef.NonEmptyString(),
            ConfigDef.Importance.HIGH,
            "The Wasm function name.")
        .define(KEY_CONVERTER,
            ConfigDef.Type.CLASS,
            ByteArrayConverter.class,
            ConfigDef.Importance.MEDIUM,
            "The converter used to serialize/deserialize the Connect Record Key to/from binary data.")
        .define(VALUE_CONVERTER,
            ConfigDef.Type.CLASS,
            ByteArrayConverter.class,
            ConfigDef.Importance.MEDIUM,
            "The converter used to serialize/deserialize the Connect Record Value to/from binary data.")
        .define(HEADER_CONVERTER,
            ConfigDef.Type.CLASS,
            ByteArrayConverter.class,
            ConfigDef.Importance.MEDIUM,
            "The converter used to serialize/deserialize the Connect Record Header to/from binary data.");

    private Module module;
    private WasmFunction<R> function;

    public WasmTransformer() {
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);

        final Converter keyConverter;
        final Converter valueConverter;
        final HeaderConverter headerConverter;


        try {
            keyConverter = (Converter) config.getClass(KEY_CONVERTER).getDeclaredConstructor().newInstance();
            valueConverter = (Converter) config.getClass(VALUE_CONVERTER).getDeclaredConstructor().newInstance();
            headerConverter = (HeaderConverter) config.getClass(HEADER_CONVERTER).getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new ConfigException("TODO");
        }

        String modulePath = config.getString(WASM_MODULE_PATH);
        if (modulePath == null) {
            throw new ConfigException(WASM_MODULE_PATH);
        }

        String functionName = config.getString(WASM_FUNCTION_NAME);
        if (functionName == null) {
            throw new ConfigException(WASM_FUNCTION_NAME);
        }

        this.module = Module.builder(new File(modulePath)).build();
        this.function = new WasmFunction<>(this.module, functionName, keyConverter, valueConverter, headerConverter);
    }

    @Override
    public R apply(R record) {
        try {
            return this.function.apply(record);
        } catch (WASMMachineException e) {
            LOGGER.warn("message: {}, stack {}", e.getMessage(), e.stackFrames());
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (this.function != null) {
            try {
                this.function.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            this.function = null;
            this.module = null;
        }
    }
}
