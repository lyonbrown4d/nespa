package io.github.lyonbrown4d.nespa;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class DeleteOptions {
    long expectedVersion;
}
