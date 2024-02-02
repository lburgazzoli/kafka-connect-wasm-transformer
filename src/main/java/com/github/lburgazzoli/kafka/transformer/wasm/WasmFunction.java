package com.github.lburgazzoli.kafka.transformer.wasm;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dylibso.chicory.runtime.ExportFunction;
import com.dylibso.chicory.runtime.HostFunction;
import com.dylibso.chicory.runtime.HostImports;
import com.dylibso.chicory.runtime.Instance;
import com.dylibso.chicory.runtime.Module;
import com.dylibso.chicory.runtime.exceptions.WASMMachineException;
import com.dylibso.chicory.wasm.types.Value;
import com.dylibso.chicory.wasm.types.ValueType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

public class WasmFunction<R extends ConnectRecord<R>> implements AutoCloseable, Function<R, R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WasmFunction.class);

    public static final ObjectMapper MAPPER = JsonMapper.builder().build();

    public static final String MODULE_NAME = "env";
    public static final String FN_ALLOC = "alloc";
    public static final String FN_DEALLOC = "dealloc";

    private final Module module;
    private final String functionName;

    private final WasmRecordConverter<R> recordConverter;

    private final Instance instance;
    private final ExportFunction function;
    private final ExportFunction alloc;
    private final ExportFunction dealloc;

    private final AtomicReference<R> ref;

    public WasmFunction(
        Module module,
        String functionName,
        Converter keyConverter,
        Converter valueConverter,
        HeaderConverter headerConverter) {

        this.ref = new AtomicReference<>();
        this.recordConverter = new WasmRecordConverter<>(keyConverter, valueConverter, headerConverter);
        this.module = Objects.requireNonNull(module);
        this.functionName = Objects.requireNonNull(functionName);
        this.instance = this.module.instantiate(imports());
        this.function = this.instance.export(this.functionName);
        this.alloc = this.instance.export(FN_ALLOC);
        this.dealloc = this.instance.export(FN_DEALLOC);
    }

    @Override
    public R apply(R record) {
        try {
            ref.set(record);

            Value[] results = function.apply();

            if (results != null) {
                int outAddr = -1;
                int outSize = 0;

                try {
                    long ptrAndSize = results[0].asLong();

                    outAddr = (int) (ptrAndSize >> 32);
                    outSize = (int) ptrAndSize;

                    // assume the max output is 31 bit, leverage the first bit for
                    // error detection
                    if (isError(outSize)) {
                        int errSize = errSize(outSize);
                        String errData = instance.memory().readString(outAddr, errSize);

                        throw new WasmFunctionException(this.functionName, errData);
                    }
                } finally {
                    if (outAddr != -1) {
                        dealloc.apply(Value.i32(outAddr), Value.i32(outSize));
                    }
                }
            }

            return ref.get();
        } catch (WASMMachineException e) {
            LOGGER.warn("message: {}, stack {}", e.getMessage(), e.stackFrames());
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            ref.set(null);
        }
    }

    @Override
    public void close() throws Exception {
    }

    private static boolean isError(int number) {
        return (number & (1 << 31)) != 0;
    }

    private static int errSize(int number) {
        return number & (~(1 << 31));
    }

    private HostImports imports() {
        HostFunction[] functions = new HostFunction[] {
                new HostFunction(
                    this::getHeaderFn,
                    MODULE_NAME,
                    "get_header",
                    List.of(ValueType.I32, ValueType.I32),
                    List.of(ValueType.I64)),
                new HostFunction(
                    this::setHeaderFn,
                    MODULE_NAME,
                    "set_header",
                    List.of(ValueType.I32, ValueType.I32, ValueType.I32, ValueType.I32),
                    List.of()),
                new HostFunction(
                    this::getKeyFn,
                    MODULE_NAME,
                    "get_key",
                    List.of(),
                    List.of(ValueType.I64)),
                new HostFunction(
                    this::setKeyFn,
                    MODULE_NAME,
                    "set_key",
                    List.of(ValueType.I32, ValueType.I32),
                    List.of()),
                new HostFunction(
                    this::getValueFn,
                    MODULE_NAME,
                    "get_value",
                    List.of(),
                    List.of(ValueType.I64)),
                new HostFunction(
                    this::setValueFn,
                    MODULE_NAME,
                    "set_value",
                    List.of(ValueType.I32, ValueType.I32),
                    List.of()),
                new HostFunction(
                    this::getTopicFn,
                    MODULE_NAME,
                    "get_topic",
                    List.of(),
                    List.of(ValueType.I64)),
                new HostFunction(
                    this::setTopicFn,
                    MODULE_NAME,
                    "set_topic",
                    List.of(ValueType.I32, ValueType.I32),
                    List.of()),
                new HostFunction(
                    this::getRecordFn,
                    MODULE_NAME,
                    "get_record",
                    List.of(),
                    List.of(ValueType.I64)),
                new HostFunction(
                    this::setRecordFn,
                    MODULE_NAME,
                    "set_record",
                    List.of(ValueType.I32, ValueType.I32),
                    List.of())
        };

        return new HostImports(functions);
    }

    /**
     * Write the give data to Wasm's linear memory.
     *
     * @param  data the data to be written
     * @return      an i64 holding the address and size fo the written data
     */
    private Value write(byte[] data) {
        int rawDataAddr = alloc.apply(Value.i32(data.length))[0].asInt();

        instance.memory().write(rawDataAddr, data);

        long ptrAndSize = rawDataAddr;
        ptrAndSize = ptrAndSize << 32;
        ptrAndSize = ptrAndSize | data.length;

        return Value.i64(ptrAndSize);

    }

    //
    // Functions
    //
    // Memory must be de-allocated by the Wasm Module
    //

    //
    // Headers
    //

    private Value[] getHeaderFn(Instance instance, Value... args) {
        final int addr = args[0].asInt();
        final int size = args[1].asInt();

        final String headerName = instance.memory().readString(addr, size);
        final R record = this.ref.get();
        final byte[] rawData = recordConverter.fromConnectHeader(record, headerName);

        return new Value[] {
                write(rawData)
        };
    }

    private Value[] setHeaderFn(Instance instance, Value... args) {
        final int headerNameAddr = args[0].asInt();
        final int headerNameSize = args[1].asInt();
        final int headerDataAddr = args[2].asInt();
        final int headerDataSize = args[3].asInt();

        final String headerName = instance.memory().readString(headerNameAddr, headerNameSize);
        final byte[] headerData = instance.memory().readBytes(headerDataAddr, headerDataSize);

        final R record = this.ref.get();
        final SchemaAndValue sv = recordConverter.toConnectHeader(record, headerName, headerData);

        record.headers().add(headerName, sv);

        return new Value[] {};
    }

    //
    // Key
    //

    private Value[] getKeyFn(Instance instance, Value... args) {
        final R record = this.ref.get();
        final byte[] rawData = recordConverter.fromConnectKey(record);

        return new Value[] {
                write(rawData)
        };
    }

    private Value[] setKeyFn(Instance instance, Value... args) {
        final int addr = args[0].asInt();
        final int size = args[1].asInt();
        final R record = this.ref.get();
        final SchemaAndValue sv = recordConverter.toConnectKey(record, instance.memory().readBytes(addr, size));

        this.ref.set(
            record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                sv.schema(),
                sv.value(),
                record.valueSchema(),
                record.value(),
                record.timestamp(),
                record.headers()));

        return new Value[] {};
    }

    //
    // Value
    //

    private Value[] getValueFn(Instance instance, Value... args) {
        final R record = this.ref.get();
        final byte[] rawData = recordConverter.fromConnectValue(record);

        return new Value[] {
                write(rawData)
        };
    }

    private Value[] setValueFn(Instance instance, Value... args) {
        final int addr = args[0].asInt();
        final int size = args[1].asInt();
        final R record = this.ref.get();
        final SchemaAndValue sv = recordConverter.toConnectValue(record, instance.memory().readBytes(addr, size));

        this.ref.set(
            record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                sv.schema(),
                sv.value(),
                record.timestamp(),
                record.headers()));

        return new Value[] {};
    }

    //
    // Topic
    //

    private Value[] getTopicFn(Instance instance, Value... args) {
        final R record = this.ref.get();
        byte[] rawData = record.topic().getBytes(StandardCharsets.UTF_8);

        return new Value[] {
                write(rawData)
        };
    }

    private Value[] setTopicFn(Instance instance, Value... args) {
        final int addr = args[0].asInt();
        final int size = args[1].asInt();
        final R record = this.ref.get();

        this.ref.set(
            record.newRecord(
                instance.memory().readString(addr, size),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                record.value(),
                record.timestamp(),
                record.headers()));

        return new Value[] {};
    }

    //
    // Record
    //

    private Value[] getRecordFn(Instance instance, Value... args) {
        final R record = this.ref.get();

        WasmRecord env = new WasmRecord();
        env.topic = record.topic();
        env.key = recordConverter.fromConnectKey(record);
        env.value = recordConverter.fromConnectValue(record);

        if (record.headers() != null) {
            // May not be needed but looks like the record headers may be required
            // by key/val converters
            for (Header header : record.headers()) {
                env.headers.put(header.key(), recordConverter.fromConnectHeader(record, header));
            }
        }

        try {
            byte[] rawData = MAPPER.writeValueAsBytes(env);

            return new Value[] {
                    write(rawData)
            };
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Value[] setRecordFn(Instance instance, Value... args) {
        final int addr = args[0].asInt();
        final int size = args[1].asInt();
        final R record = this.ref.get();
        final byte[] in = instance.memory().readBytes(addr, size);

        try {
            WasmRecord w = MAPPER.readValue(in, WasmRecord.class);

            // May not be needed but looks like the record headers may be required
            // by key/val converters so let's do it even if I don't think the way
            // I'm doing it is 100% correct :)

            Headers connectHeaders = new ConnectHeaders();

            w.headers.forEach((k, v) -> {
                connectHeaders.add(k, recordConverter.toConnectHeader(record, k, v));
            });

            SchemaAndValue keyAndSchema = recordConverter.toConnectKey(record, w.key);
            SchemaAndValue valueAndSchema = recordConverter.toConnectValue(record, w.value);

            this.ref.set(
                record.newRecord(
                    w.topic,
                    record.kafkaPartition(),
                    keyAndSchema.schema(),
                    keyAndSchema.value(),
                    valueAndSchema.schema(),
                    valueAndSchema.value(),
                    record.timestamp(),
                    connectHeaders));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new Value[] {};
    }
}
