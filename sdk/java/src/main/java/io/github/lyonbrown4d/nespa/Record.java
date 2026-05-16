package io.github.lyonbrown4d.nespa;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class Record {
    boolean found;
    @Builder.Default String namespace = "";
    @Builder.Default String space = "";
    @Builder.Default String entity = "";
    @Builder.Default String key = "";
    @Builder.Default byte[] value = new byte[0];
    long version;
    long namespaceVersion;
    long spaceVersion;
    long expireAtUnixMs;
}
