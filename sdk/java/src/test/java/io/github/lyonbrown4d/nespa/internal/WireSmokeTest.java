package io.github.lyonbrown4d.nespa.internal;

import io.github.lyonbrown4d.nespa.Key;
import io.github.lyonbrown4d.nespa.PrimitiveKind;
import io.github.lyonbrown4d.nespa.PrimitiveRequest;
import io.github.lyonbrown4d.nespa.PrimitiveResult;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

public final class WireSmokeTest {
    private static final byte[] VALUE = "alice".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIELD_VALUE = "admin".getBytes(StandardCharsets.UTF_8);

    private WireSmokeTest() {
    }

    public static void main(String[] args) {
        smokePrimitiveRequest();
        smokePrimitiveResponse();
        smokeBatchPrimitiveRequest();
        smokeBatchPrimitiveResponse();
    }

    private static void smokePrimitiveRequest() {
        WirePayload encoded = CacheWire.encodePrimitiveRequest(mapSetRequest("profile", VALUE));
        require(encoded.metadata().length > 1, "primitive request metadata should not be empty");
        require(Arrays.equals(encoded.payload(), VALUE), "primitive request payload mismatch");
    }

    private static void smokePrimitiveResponse() {
        PrimitiveResult result = CacheWire.decodePrimitiveResponse(responseMetadata(), responsePayload());
        require(result.isFound(), "primitive result should be found");
        require(result.isApplied(), "primitive result should be applied");
        require("profile".equals(result.getRecord().getKey()), "primitive record key mismatch");
        require(Arrays.equals(result.getValue(), VALUE), "primitive value mismatch");
        require(result.getFields().size() == 1, "primitive field count mismatch");
        require(Arrays.equals(result.getFields().getFirst().getValue(), FIELD_VALUE), "primitive field value mismatch");
        require(result.getMembers().contains("blue"), "primitive member missing");
        require(result.getScoredMembers().getFirst().getScore() == 42.0, "primitive score mismatch");
        require(result.getValues().isEmpty(), "primitive list values should be empty");
    }

    private static void smokeBatchPrimitiveRequest() {
        List<PrimitiveRequest> requests = List.of(
                mapSetRequest("profile", VALUE),
                mapSetRequest("settings", FIELD_VALUE));
        WirePayload encoded = CacheWire.encodeBatchPrimitiveRequest(requests);
        require(encoded.metadata().length > 1, "batch primitive metadata should not be empty");
        require(Arrays.equals(encoded.payload(), concat(VALUE, FIELD_VALUE)), "batch primitive payload mismatch");
    }

    private static void smokeBatchPrimitiveResponse() {
        CacheWire.Writer out = new CacheWire.Writer();
        out.writeUint64(2);
        writePrimitiveResult(out, "profile", 0, VALUE.length);
        writePrimitiveResult(out, "settings", VALUE.length, FIELD_VALUE.length);

        List<PrimitiveResult> results = CacheWire.decodeBatchPrimitiveResponse(out.bytes(), concat(VALUE, FIELD_VALUE));
        require(results.size() == 2, "batch primitive result count mismatch");
        require("profile".equals(results.getFirst().getRecord().getKey()), "first primitive result key mismatch");
        require("settings".equals(results.get(1).getRecord().getKey()), "second primitive result key mismatch");
    }

    private static PrimitiveRequest mapSetRequest(String key, byte[] value) {
        return PrimitiveRequest.builder()
                .kind(PrimitiveKind.MAP_SET)
                .key(Key.builder().namespace("orders").space("session").key(key).build())
                .field("name")
                .value(value)
                .start(1)
                .options(io.github.lyonbrown4d.nespa.PrimitiveOptions.builder()
                        .ttl(Duration.ofSeconds(5))
                        .namespaceVersion(7)
                        .spaceVersion(11)
                        .expectedVersion(13)
                        .build())
                .build();
    }

    private static byte[] responseMetadata() {
        CacheWire.Writer out = new CacheWire.Writer();
        writePrimitiveResult(out, "profile", 0, VALUE.length);
        return out.bytes();
    }

    private static byte[] responsePayload() {
        return concat(VALUE, FIELD_VALUE);
    }

    private static void writePrimitiveResult(CacheWire.Writer out, String key, int valueOffset, int valueSize) {
        out.writeBool(true);
        out.writeBool(true);
        writeRecord(out, key);
        out.writeBool(true);
        out.writeUint64(2);
        out.writeUint32(valueOffset);
        out.writeUint32(valueSize);
        out.writeUint64(1);
        out.writeString("role");
        out.writeUint32(VALUE.length);
        out.writeUint32(FIELD_VALUE.length);
        out.writeUint64(1);
        out.writeString("blue");
        out.writeUint64(1);
        out.writeString("alice");
        out.writeFloat64(42.0);
        out.writeUint64(0);
    }

    private static void writeRecord(CacheWire.Writer out, String key) {
        out.writeBool(true);
        out.writeString("orders");
        out.writeString("session");
        out.writeString("");
        out.writeString(key);
        out.writeUint64(3);
        out.writeUint64(7);
        out.writeUint64(11);
        out.writeInt64(0);
        out.writeUint32(0);
        out.writeUint32(0);
    }

    private static byte[] concat(byte[] first, byte[] second) {
        byte[] out = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, out, first.length, second.length);
        return out;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
