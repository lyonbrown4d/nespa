package io.github.lyonbrown4d.nespa;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class MapField {
    @Builder.Default String field = "";
    @Builder.Default byte[] value = new byte[0];
}
