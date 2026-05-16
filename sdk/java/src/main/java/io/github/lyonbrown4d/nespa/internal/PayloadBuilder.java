package io.github.lyonbrown4d.nespa.internal;

import java.io.ByteArrayOutputStream;

final class PayloadBuilder {
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    PayloadRange append(byte[] value) {
        byte[] raw = value == null ? new byte[0] : value;
        int offset = out.size();
        out.writeBytes(raw);
        return new PayloadRange(offset, raw.length);
    }

    byte[] bytes() {
        return out.toByteArray();
    }
}
