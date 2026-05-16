package io.github.lyonbrown4d.nespa;

import java.io.IOException;

public interface CacheClient extends AutoCloseable {
    Record set(Key key, byte[] value, SetOptions options) throws IOException;

    Record get(Key key, GetOptions options) throws IOException;

    boolean delete(Key key, DeleteOptions options) throws IOException;

    boolean exists(Key key, GetOptions options) throws IOException;

    boolean touch(Key key, TouchOptions options) throws IOException;

    Record adjust(Key key, AdjustOptions options) throws IOException;

    @Override
    void close();
}
