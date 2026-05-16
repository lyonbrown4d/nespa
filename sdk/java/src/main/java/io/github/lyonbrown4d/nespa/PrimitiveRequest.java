package io.github.lyonbrown4d.nespa;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
public class PrimitiveRequest {
    @NonNull PrimitiveKind kind;
    @NonNull Key key;
    @Builder.Default PrimitiveOptions options = PrimitiveOptions.builder().build();
    @Builder.Default String field = "";
    @Builder.Default String member = "";
    @Builder.Default byte[] value = new byte[0];
    long delta;
    long initialValue;
    double score;
    double minScore;
    double maxScore;
    boolean hasMinScore;
    boolean hasMaxScore;
    long limit;
    long start;
    boolean reverse;
}
