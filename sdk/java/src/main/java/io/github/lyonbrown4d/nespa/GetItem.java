package io.github.lyonbrown4d.nespa;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
public class GetItem {
    @NonNull Key key;
    @Builder.Default GetOptions options = GetOptions.builder().build();
}
