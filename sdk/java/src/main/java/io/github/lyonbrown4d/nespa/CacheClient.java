package io.github.lyonbrown4d.nespa;

import java.io.IOException;
import java.util.List;

public interface CacheClient extends AutoCloseable {
    Record set(Key key, byte[] value, SetOptions options) throws IOException;

    Record get(Key key, GetOptions options) throws IOException;

    boolean delete(Key key, DeleteOptions options) throws IOException;

    boolean exists(Key key, GetOptions options) throws IOException;

    boolean touch(Key key, TouchOptions options) throws IOException;

    Record adjust(Key key, AdjustOptions options) throws IOException;

    List<Record> batchSet(List<SetItem> items) throws IOException;

    List<Record> batchGet(List<GetItem> items) throws IOException;

    PrimitiveResult primitive(PrimitiveRequest request) throws IOException;

    List<PrimitiveResult> batchPrimitive(List<PrimitiveRequest> requests) throws IOException;

    @Override
    void close();
}
