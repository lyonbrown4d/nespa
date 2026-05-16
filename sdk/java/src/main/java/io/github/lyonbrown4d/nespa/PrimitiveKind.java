package io.github.lyonbrown4d.nespa;

public enum PrimitiveKind {
    COUNTER_ADJUST(1),
    MAP_SET(2),
    MAP_GET(3),
    MAP_DELETE(4),
    MAP_GET_ALL(5),
    SET_ADD(6),
    SET_REMOVE(7),
    SET_CONTAINS(8),
    SET_MEMBERS(9),
    SCORED_SET_PUT(10),
    SCORED_SET_REMOVE(11),
    SCORED_SET_RANGE(12),
    LIST_PUSH_FRONT(13),
    LIST_PUSH_BACK(14),
    LIST_POP_FRONT(15),
    LIST_POP_BACK(16),
    LIST_RANGE(17);

    private final int code;

    PrimitiveKind(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }
}
