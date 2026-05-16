package io.github.lyonbrown4d.nespa;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
public class SetItem {
    @NonNull Key key;
    @Builder.Default byte[] value = new byte[0];
    @Builder.Default SetOptions options = SetOptions.builder().build();
}
