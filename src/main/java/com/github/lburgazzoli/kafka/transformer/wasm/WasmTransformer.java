package com.github.lburgazzoli.kafka.transformer.wasm;

import java.io.File;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import com.dylibso.chicory.runtime.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

public class WasmTransformer<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {
    public static final ObjectMapper MAPPER = JsonMapper.builder().build();

    public static final String OVERVIEW_DOC = "Apply a wasm function to a Kafka Connect Record.";

    public static final String KEY_CONVERTER = "key.converter";
    public static final String VALUE_CONVERTER = "value.converter";
    public static final String HEADER_CONVERTER = "header.converter";
    public static final String WASM_MODULE_PATH = "wasm.module.path";
    public static final String WASM_FUNCTION_NAME = "wasm.function.name";

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

    private Converter keyConverter;
    private Converter valueConverter;
    private HeaderConverter headerConverter;

    private Module module;
    private WasmFunction function;

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

        try {
            this.keyConverter = (Converter) config.getClass(KEY_CONVERTER).getDeclaredConstructor().newInstance();
            this.valueConverter = (Converter) config.getClass(VALUE_CONVERTER).getDeclaredConstructor().newInstance();
            this.headerConverter = (HeaderConverter) config.getClass(HEADER_CONVERTER).getDeclaredConstructor().newInstance();
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
        this.function = new WasmFunction(this.module, functionName);
    }

    @Override
    public R apply(R record) {
        try {
            byte[] in = serialize(record);
            byte[] result = this.function.run(in);
            R out = deserialize(record, result);

            return out;
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

    private byte[] serialize(R record) throws Exception {
        // May not be needed but looks like the record headers may be required
        // by key/val converters
        RecordHeaders recordHeaders = new RecordHeaders();
        if (record.headers() != null) {
            String topic = record.topic();
            for (Header header : record.headers()) {
                String key = header.key();
                byte[] rawHeader = this.headerConverter.fromConnectHeader(topic, key, header.schema(), header.value());
                recordHeaders.add(key, rawHeader);
            }
        }

        WasmRecord env = new WasmRecord();
        env.topic = record.topic();
        env.key = keyConverter.fromConnectData(record.topic(), recordHeaders, record.keySchema(), record.key());
        env.value = valueConverter.fromConnectData(record.topic(), recordHeaders, record.valueSchema(), record.value());

        recordHeaders.forEach(h -> {
            env.headers.put(h.key(), h.value());
        });

        return MAPPER.writeValueAsBytes(env);
    }

    private R deserialize(R record, byte[] in) throws Exception {

        WasmRecord w = MAPPER.readValue(in, WasmRecord.class);

        RecordHeaders recordHeaders = new RecordHeaders();
        Headers connectHeaders = new ConnectHeaders();

        // May not be needed but looks like the record headers may be required
        // by key/val converters so let's do it even if I don't think the way
        // I'm doing it is 100% correct :)
        w.headers.forEach((k, v) -> {
            recordHeaders.add(k, v);
            connectHeaders.add(k, headerConverter.toConnectHeader(w.topic, k, v));
        });

        SchemaAndValue keyAndSchema = keyConverter.toConnectData(w.topic, recordHeaders, w.key);
        SchemaAndValue valueAndSchema = valueConverter.toConnectData(w.topic, recordHeaders, w.value);

        return record.newRecord(
            w.topic,
            record.kafkaPartition(),
            keyAndSchema.schema(),
            keyAndSchema.value(),
            valueAndSchema.schema(),
            valueAndSchema.value(),
            record.timestamp(),
            connectHeaders);
    }
}
