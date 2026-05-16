package io.github.lyonbrown4d.nespa.internal;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class Frame {
    int flags;
    int op;
    long requestId;
    long routeEpoch;
    @Builder.Default byte[] metadata = new byte[0];
    @Builder.Default byte[] payload = new byte[0];
}
