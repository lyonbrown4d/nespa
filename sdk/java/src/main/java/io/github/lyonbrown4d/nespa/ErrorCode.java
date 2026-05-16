package io.github.lyonbrown4d.nespa;

import io.github.lyonbrown4d.nespa.internal.Protocol;

public final class ErrorCode {
    public static final int UNKNOWN = Protocol.ERROR_UNKNOWN;
    public static final int BAD_FRAME = Protocol.ERROR_BAD_FRAME;
    public static final int UNSUPPORTED_VERSION = Protocol.ERROR_UNSUPPORTED_VERSION;
    public static final int TOO_LARGE = Protocol.ERROR_TOO_LARGE;
    public static final int NO_ROUTE = Protocol.ERROR_NO_ROUTE;
    public static final int TIMEOUT = Protocol.ERROR_TIMEOUT;
    public static final int UNAVAILABLE = Protocol.ERROR_UNAVAILABLE;
    public static final int INTERNAL = Protocol.ERROR_INTERNAL;
    public static final int INVALID_ARGUMENT = Protocol.ERROR_INVALID_ARGUMENT;

    private ErrorCode() {
    }
}
