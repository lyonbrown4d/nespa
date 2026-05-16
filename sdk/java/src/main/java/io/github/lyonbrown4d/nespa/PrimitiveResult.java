package io.github.lyonbrown4d.nespa;

import java.util.List;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class PrimitiveResult {
    @Builder.Default Record record = Record.builder().build();
    boolean found;
    boolean applied;
    @Builder.Default byte[] value = new byte[0];
    boolean boolValue;
    long count;
    @Builder.Default List<MapField> fields = List.of();
    @Builder.Default List<String> members = List.of();
    @Builder.Default List<ScoredMember> scoredMembers = List.of();
    @Builder.Default List<byte[]> values = List.of();
}
