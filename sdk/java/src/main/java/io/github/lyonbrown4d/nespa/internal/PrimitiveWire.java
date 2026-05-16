package io.github.lyonbrown4d.nespa.internal;

import io.github.lyonbrown4d.nespa.MapField;
import io.github.lyonbrown4d.nespa.PrimitiveRequest;
import io.github.lyonbrown4d.nespa.PrimitiveResult;
import io.github.lyonbrown4d.nespa.Record;
import io.github.lyonbrown4d.nespa.ScoredMember;
import java.util.ArrayList;
import java.util.List;

final class PrimitiveWire {
    private PrimitiveWire() {
    }

    static WirePayload encodeRequest(PrimitiveRequest request) {
        byte[] payload = copy(request.getValue());
        CacheWire.Writer out = new CacheWire.Writer();
        writeRequest(out, request, 0, payload.length);
        return new WirePayload(out.bytes(), payload);
    }

    static WirePayload encodeBatchRequest(List<PrimitiveRequest> requests) {
        List<PrimitiveRequest> items = requests == null ? List.of() : requests;
        CacheWire.Writer out = new CacheWire.Writer();
        PayloadBuilder payload = new PayloadBuilder();
        out.writeUint64(items.size());
        for (PrimitiveRequest request : items) {
            PayloadRange range = payload.append(request.getValue());
            writeRequest(out, request, range.offset(), range.size());
        }
        return new WirePayload(out.bytes(), payload.bytes());
    }

    static PrimitiveResult decodeResponse(byte[] metadata, byte[] payload) {
        CacheWire.Reader in = new CacheWire.Reader(metadata);
        PrimitiveResult result = readResult(in, safePayload(payload));
        in.ensureEOF();
        return result;
    }

    static List<PrimitiveResult> decodeBatchResponse(byte[] metadata, byte[] payload) {
        CacheWire.Reader in = new CacheWire.Reader(metadata);
        int count = checkedCount(in.readUint64());
        List<PrimitiveResult> results = new ArrayList<>(count);
        byte[] rawPayload = safePayload(payload);
        for (int index = 0; index < count; index++) {
            results.add(readResult(in, rawPayload));
        }
        in.ensureEOF();
        return List.copyOf(results);
    }

    private static void writeRequest(CacheWire.Writer out, PrimitiveRequest request, int payloadOffset, int payloadSize) {
        out.writeByte(request.getKind().code());
        out.writeKey(request.getKey());
        out.writeInt64(CacheWire.millis(request.getOptions().getTtl()));
        out.writeUint64(request.getOptions().getNamespaceVersion());
        out.writeUint64(request.getOptions().getSpaceVersion());
        out.writeUint64(request.getOptions().getExpectedVersion());
        out.writeString(request.getField());
        out.writeString(request.getMember());
        out.writeInt64(request.getDelta());
        out.writeInt64(request.getInitialValue());
        out.writeFloat64(request.getScore());
        out.writeFloat64(request.getMinScore());
        out.writeFloat64(request.getMaxScore());
        out.writeUint64(request.getLimit());
        out.writeInt64(request.getStart());
        out.writeBool(request.isHasMinScore());
        out.writeBool(request.isHasMaxScore());
        out.writeBool(request.isReverse());
        out.writeUint32(payloadOffset);
        out.writeUint32(payloadSize);
    }

    private static PrimitiveResult readResult(CacheWire.Reader in, byte[] payload) {
        boolean found = in.readBool();
        boolean applied = in.readBool();
        Record record = readBatchRecord(in);
        boolean boolValue = in.readBool();
        long count = in.readUint64();
        byte[] value = readPayload(in, payload);
        List<MapField> fields = readFields(in, payload);
        List<String> members = readMembers(in);
        List<ScoredMember> scoredMembers = readScoredMembers(in);
        List<byte[]> values = readValues(in, payload);
        return PrimitiveResult.builder()
                .record(record)
                .found(found)
                .applied(applied)
                .value(value)
                .boolValue(boolValue)
                .count(count)
                .fields(fields)
                .members(members)
                .scoredMembers(scoredMembers)
                .values(values)
                .build();
    }

    private static Record readBatchRecord(CacheWire.Reader in) {
        boolean found = in.readBool();
        if (!found) {
            return Record.builder().found(false).build();
        }
        Record record = readRecordFields(in);
        in.readUint32();
        in.readUint32();
        return record;
    }

    private static Record readRecordFields(CacheWire.Reader in) {
        return Record.builder()
                .found(true)
                .namespace(in.readString())
                .space(in.readString())
                .entity(in.readString())
                .key(in.readString())
                .version(in.readUint64())
                .namespaceVersion(in.readUint64())
                .spaceVersion(in.readUint64())
                .expireAtUnixMs(in.readInt64())
                .build();
    }

    private static List<MapField> readFields(CacheWire.Reader in, byte[] payload) {
        int count = checkedCount(in.readUint64());
        List<MapField> fields = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            fields.add(MapField.builder()
                    .field(in.readString())
                    .value(readPayload(in, payload))
                    .build());
        }
        return List.copyOf(fields);
    }

    private static List<String> readMembers(CacheWire.Reader in) {
        int count = checkedCount(in.readUint64());
        List<String> members = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            members.add(in.readString());
        }
        return List.copyOf(members);
    }

    private static List<ScoredMember> readScoredMembers(CacheWire.Reader in) {
        int count = checkedCount(in.readUint64());
        List<ScoredMember> members = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            members.add(ScoredMember.builder()
                    .member(in.readString())
                    .score(in.readFloat64())
                    .build());
        }
        return List.copyOf(members);
    }

    private static List<byte[]> readValues(CacheWire.Reader in, byte[] payload) {
        int count = checkedCount(in.readUint64());
        List<byte[]> values = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            values.add(readPayload(in, payload));
        }
        return List.copyOf(values);
    }

    private static byte[] readPayload(CacheWire.Reader in, byte[] payload) {
        int offset = in.readUint32();
        int size = in.readUint32();
        if (offset < 0 || size < 0 || offset > payload.length || payload.length - offset < size) {
            throw new IllegalArgumentException("cachewire: invalid payload range");
        }
        byte[] value = new byte[size];
        System.arraycopy(payload, offset, value, 0, size);
        return value;
    }

    private static int checkedCount(long count) {
        if (count < 0 || count > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("cachewire: count exceeds metadata");
        }
        return Math.toIntExact(count);
    }

    private static byte[] safePayload(byte[] payload) {
        return payload == null ? new byte[0] : payload;
    }

    private static byte[] copy(byte[] value) {
        return value == null || value.length == 0 ? new byte[0] : value.clone();
    }
}
