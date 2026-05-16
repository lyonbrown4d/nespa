package io.github.lyonbrown4d.nespa;

import java.time.Duration;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class TouchOptions {
    @Builder.Default Duration ttl = Duration.ZERO;
    long namespaceVersion;
    long spaceVersion;
    long expectedVersion;
}
