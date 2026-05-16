package io.github.lyonbrown4d.nespa.internal;

import io.github.lyonbrown4d.nespa.GetItem;
import io.github.lyonbrown4d.nespa.Record;
import io.github.lyonbrown4d.nespa.SetItem;
import java.util.ArrayList;
import java.util.List;

final class BatchWire {
    private BatchWire() {
    }

    static WirePayload encodeSetRequest(List<SetItem> items) {
        List<SetItem> requests = items == null ? List.of() : items;
        CacheWire.Writer out = new CacheWire.Writer();
        PayloadBuilder payload = new PayloadBuilder();
        out.writeUint64(requests.size());
        for (SetItem item : requests) {
            PayloadRange range = payload.append(item.getValue());
            writeSetItem(out, item, range);
        }
        return new WirePayload(out.bytes(), payload.bytes());
    }

    static byte[] encodeGetRequest(List<GetItem> items) {
        List<GetItem> requests = items == null ? List.of() : items;
        CacheWire.Writer out = new CacheWire.Writer();
        out.writeUint64(requests.size());
        for (GetItem item : requests) {
            writeGetItem(out, item);
        }
        return out.bytes();
    }

    static List<Record> decodeRecords(byte[] metadata, byte[] payload) {
        CacheWire.Reader in = new CacheWire.Reader(metadata);
        int count = checkedCount(in.readUint64());
        List<Record> records = new ArrayList<>(count);
        byte[] rawPayload = safePayload(payload);
        for (int index = 0; index < count; index++) {
            records.add(readRecord(in, rawPayload));
        }
        in.ensureEOF();
        return List.copyOf(records);
    }

    private static void writeSetItem(CacheWire.Writer out, SetItem item, PayloadRange range) {
        out.writeKey(item.getKey());
        out.writeInt64(CacheWire.millis(item.getOptions().getTtl()));
        out.writeUint64(item.getOptions().getNamespaceVersion());
        out.writeUint64(item.getOptions().getSpaceVersion());
        out.writeUint64(item.getOptions().getExpectedVersion());
        out.writeUint32(range.offset());
        out.writeUint32(range.size());
    }

    private static void writeGetItem(CacheWire.Writer out, GetItem item) {
        out.writeKey(item.getKey());
        out.writeUint64(item.getOptions().getNamespaceVersion());
        out.writeUint64(item.getOptions().getSpaceVersion());
    }

    private static Record readRecord(CacheWire.Reader in, byte[] payload) {
        boolean found = in.readBool();
        if (!found) {
            return Record.builder().found(false).build();
        }
        return readRecordFields(in, readPayload(in, payload));
    }

    private static Record readRecordFields(CacheWire.Reader in, byte[] value) {
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
                .value(value)
                .build();
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
}
