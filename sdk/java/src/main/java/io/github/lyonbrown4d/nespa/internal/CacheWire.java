package io.github.lyonbrown4d.nespa.internal;

import io.github.lyonbrown4d.nespa.AdjustOptions;
import io.github.lyonbrown4d.nespa.DeleteOptions;
import io.github.lyonbrown4d.nespa.GetOptions;
import io.github.lyonbrown4d.nespa.Key;
import io.github.lyonbrown4d.nespa.PrimitiveRequest;
import io.github.lyonbrown4d.nespa.PrimitiveResult;
import io.github.lyonbrown4d.nespa.Record;
import io.github.lyonbrown4d.nespa.SetOptions;
import io.github.lyonbrown4d.nespa.TouchOptions;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;

public final class CacheWire {
    private static final byte METADATA_VERSION = 1;
    private static final byte BOOL_FALSE = 0;
    private static final byte BOOL_TRUE = 1;

    private CacheWire() {
    }

    public static byte[] encodeSetRequest(Key key, SetOptions options) {
        Writer out = new Writer();
        out.writeKey(key);
        out.writeInt64(millis(options.getTtl()));
        out.writeUint64(options.getNamespaceVersion());
        out.writeUint64(options.getSpaceVersion());
        out.writeUint64(options.getExpectedVersion());
        return out.bytes();
    }

    public static byte[] encodeGetRequest(Key key, GetOptions options) {
        Writer out = new Writer();
        out.writeKey(key);
        out.writeUint64(options.getNamespaceVersion());
        out.writeUint64(options.getSpaceVersion());
        return out.bytes();
    }

    public static byte[] encodeDeleteRequest(Key key, DeleteOptions options) {
        Writer out = new Writer();
        out.writeKey(key);
        out.writeUint64(options.getExpectedVersion());
        return out.bytes();
    }

    public static byte[] encodeExistsRequest(Key key, GetOptions options) {
        return encodeGetRequest(key, options);
    }

    public static byte[] encodeTouchRequest(Key key, TouchOptions options) {
        Writer out = new Writer();
        out.writeKey(key);
        out.writeInt64(millis(options.getTtl()));
        out.writeUint64(options.getNamespaceVersion());
        out.writeUint64(options.getSpaceVersion());
        out.writeUint64(options.getExpectedVersion());
        return out.bytes();
    }

    public static byte[] encodeAdjustRequest(Key key, AdjustOptions options) {
        Writer out = new Writer();
        out.writeKey(key);
        out.writeInt64(millis(options.getTtl()));
        out.writeInt64(options.getInitialValue());
        out.writeInt64(options.getDelta());
        out.writeUint64(options.getNamespaceVersion());
        out.writeUint64(options.getSpaceVersion());
        out.writeUint64(options.getExpectedVersion());
        return out.bytes();
    }

    public static Record decodeRecord(byte[] metadata, byte[] payload) {
        Reader in = new Reader(metadata);
        boolean found = in.readBool();
        if (!found) {
            in.ensureEOF();
            return Record.builder().found(false).build();
        }
        String namespace = in.readString();
        String space = in.readString();
        String entity = in.readString();
        String key = in.readString();
        long version = in.readUint64();
        long namespaceVersion = in.readUint64();
        long spaceVersion = in.readUint64();
        long expireAtUnixMs = in.readInt64();
        in.ensureEOF();
        return Record.builder()
                .found(true)
                .namespace(namespace)
                .space(space)
                .entity(entity)
                .key(key)
                .version(version)
                .namespaceVersion(namespaceVersion)
                .spaceVersion(spaceVersion)
                .expireAtUnixMs(expireAtUnixMs)
                .value(payload == null ? new byte[0] : payload.clone())
                .build();
    }

    public static boolean decodeBoolean(byte[] metadata) {
        Reader in = new Reader(metadata);
        boolean value = in.readBool();
        in.ensureEOF();
        return value;
    }

    public static WirePayload encodePrimitiveRequest(PrimitiveRequest request) {
        return PrimitiveWire.encodeRequest(request);
    }

    public static PrimitiveResult decodePrimitiveResponse(byte[] metadata, byte[] payload) {
        return PrimitiveWire.decodeResponse(metadata, payload);
    }

    public static WirePayload encodeBatchPrimitiveRequest(List<PrimitiveRequest> requests) {
        return PrimitiveWire.encodeBatchRequest(requests);
    }

    public static List<PrimitiveResult> decodeBatchPrimitiveResponse(byte[] metadata, byte[] payload) {
        return PrimitiveWire.decodeBatchResponse(metadata, payload);
    }

    static long millis(java.time.Duration ttl) {
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            return 0;
        }
        return ttl.toMillis();
    }

    static final class Writer {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        Writer() {
            out.write(METADATA_VERSION);
        }

        byte[] bytes() {
            return out.toByteArray();
        }

        void writeKey(Key key) {
            writeString(key.getNamespace());
            writeString(key.getSpace());
            writeString(key.getEntity());
            writeString(key.getKey());
        }

        void writeString(String value) {
            byte[] raw = (value == null ? "" : value).getBytes(StandardCharsets.UTF_8);
            writeUvarint(raw.length);
            out.writeBytes(raw);
        }

        void writeUint64(long value) {
            out.writeBytes(ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN).putLong(value).array());
        }

        void writeUint32(int value) {
            out.writeBytes(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(value).array());
        }

        void writeByte(int value) {
            out.write(value);
        }

        void writeBool(boolean value) {
            writeByte(value ? BOOL_TRUE : BOOL_FALSE);
        }

        void writeFloat64(double value) {
            out.writeBytes(ByteBuffer.allocate(Double.BYTES).order(ByteOrder.BIG_ENDIAN).putDouble(value).array());
        }

        void writeInt64(long value) {
            long encoded = value << 1;
            if (value < 0) {
                encoded = ~encoded;
            }
            writeUvarint(encoded);
        }

        void writeUvarint(long value) {
            while ((value & ~0x7fL) != 0) {
                out.write((int) (value & 0x7f) | 0x80);
                value >>>= 7;
            }
            out.write((int) value);
        }
    }

    static final class Reader {
        private final byte[] raw;
        private int pos;

        Reader(byte[] raw) {
            this.raw = raw == null ? new byte[0] : raw;
            if (this.raw.length == 0) {
                throw new IllegalArgumentException("cachewire: missing metadata version");
            }
            if (this.raw[0] != METADATA_VERSION) {
                throw new IllegalArgumentException("cachewire: unsupported metadata version " + this.raw[0]);
            }
            this.pos = 1;
        }

        String readString() {
            long size = readUvarint();
            if (size > Integer.MAX_VALUE || pos + (int) size > raw.length) {
                throw new IllegalArgumentException("cachewire: string exceeds metadata");
            }
            String value = new String(raw, pos, (int) size, StandardCharsets.UTF_8);
            pos += (int) size;
            return value;
        }

        boolean readBool() {
            byte value = readByte();
            if (value == BOOL_FALSE) {
                return false;
            }
            if (value == BOOL_TRUE) {
                return true;
            }
            throw new IllegalArgumentException("cachewire: invalid bool " + value);
        }

        long readUint64() {
            ensureAvailable(Long.BYTES);
            long value = ByteBuffer.wrap(raw, pos, Long.BYTES).order(ByteOrder.BIG_ENDIAN).getLong();
            pos += Long.BYTES;
            return value;
        }

        int readUint32() {
            ensureAvailable(Integer.BYTES);
            int value = ByteBuffer.wrap(raw, pos, Integer.BYTES).order(ByteOrder.BIG_ENDIAN).getInt();
            pos += Integer.BYTES;
            return value;
        }

        double readFloat64() {
            ensureAvailable(Double.BYTES);
            double value = ByteBuffer.wrap(raw, pos, Double.BYTES).order(ByteOrder.BIG_ENDIAN).getDouble();
            pos += Double.BYTES;
            return value;
        }

        long readInt64() {
            long value = readUvarint();
            long decoded = value >>> 1;
            if ((value & 1) != 0) {
                decoded = ~decoded;
            }
            return decoded;
        }

        long readUvarint() {
            long x = 0;
            int shift = 0;
            for (int i = 0; i < 10; i++) {
                byte next = readByte();
                if (next >= 0) {
                    if (i == 9 && next > 1) {
                        throw new IllegalArgumentException("cachewire: varint overflow");
                    }
                    return x | ((long) next << shift);
                }
                x |= (long) (next & 0x7f) << shift;
                shift += 7;
            }
            throw new IllegalArgumentException("cachewire: varint overflow");
        }

        void ensureEOF() {
            if (pos != raw.length) {
                throw new IllegalArgumentException("cachewire: trailing bytes");
            }
        }

        byte readByte() {
            ensureAvailable(1);
            return raw[pos++];
        }

        private void ensureAvailable(int count) {
            if (pos + count > raw.length) {
                throw new IllegalArgumentException("cachewire: metadata truncated");
            }
        }
    }
}
