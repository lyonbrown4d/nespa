package io.github.lyonbrown4d.nespa.internal;

public final class Protocol {
    public static final int MAGIC = 0x4e535041;
    public static final byte VERSION = 1;
    public static final int FIXED_HEADER_SIZE = 32;

    public static final int ERROR_UNKNOWN = 1;
    public static final int ERROR_BAD_FRAME = 2;
    public static final int ERROR_UNSUPPORTED_VERSION = 3;
    public static final int ERROR_TOO_LARGE = 4;
    public static final int ERROR_NO_ROUTE = 5;
    public static final int ERROR_TIMEOUT = 6;
    public static final int ERROR_UNAVAILABLE = 7;
    public static final int ERROR_INTERNAL = 8;
    public static final int ERROR_INVALID_ARGUMENT = 9;

    public static final int OP_CACHE_GET = 1;
    public static final int OP_CACHE_SET = 2;
    public static final int OP_CACHE_DELETE = 3;
    public static final int OP_CACHE_BATCH_GET = 4;
    public static final int OP_CACHE_BATCH_SET = 5;
    public static final int OP_CACHE_EXISTS = 9;
    public static final int OP_CACHE_TOUCH = 10;
    public static final int OP_CACHE_ADJUST = 11;

    public static final int FLAG_RESPONSE = 1;
    public static final int FLAG_ERROR = 2;

    private Protocol() {
    }
}
