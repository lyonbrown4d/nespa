package io.github.lyonbrown4d.nespa;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
public class Key {
    @NonNull String namespace;
    @NonNull String space;
    @Builder.Default String entity = "";
    @NonNull String key;
}
