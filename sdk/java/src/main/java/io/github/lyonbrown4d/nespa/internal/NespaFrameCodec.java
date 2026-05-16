package io.github.lyonbrown4d.nespa.internal;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import java.io.IOException;
import java.util.List;

public class NespaFrameCodec extends ByteToMessageCodec<Frame> {
    @Override
    protected void encode(ChannelHandlerContext ctx, Frame frame, ByteBuf out) {
        byte[] metadata = bytes(frame.getMetadata());
        byte[] payload = bytes(frame.getPayload());
        out.writeInt(Protocol.MAGIC);
        out.writeByte(Protocol.VERSION);
        out.writeByte(frame.getFlags());
        out.writeShort(frame.getOp());
        out.writeLong(frame.getRequestId());
        out.writeLong(frame.getRouteEpoch());
        out.writeInt(metadata.length);
        out.writeInt(payload.length);
        out.writeBytes(metadata);
        out.writeBytes(payload);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws IOException {
        if (in.readableBytes() < Protocol.FIXED_HEADER_SIZE) {
            return;
        }

        in.markReaderIndex();
        int magic = in.readInt();
        if (magic != Protocol.MAGIC) {
            throw new IOException("protocol: invalid frame magic");
        }

        byte version = in.readByte();
        if (version != Protocol.VERSION) {
            throw new IOException("protocol: unsupported frame version " + version);
        }

        int flags = Byte.toUnsignedInt(in.readByte());
        int op = Short.toUnsignedInt(in.readShort());
        long requestId = in.readLong();
        long routeEpoch = in.readLong();
        int metadataLen = in.readInt();
        int payloadLen = in.readInt();
        if (metadataLen < 0 || payloadLen < 0) {
            throw new IOException("protocol: negative frame length");
        }
        if (in.readableBytes() < metadataLen + payloadLen) {
            in.resetReaderIndex();
            return;
        }

        byte[] metadata = readBytes(in, metadataLen);
        byte[] payload = readBytes(in, payloadLen);
        out.add(Frame.builder()
                .flags(flags)
                .op(op)
                .requestId(requestId)
                .routeEpoch(routeEpoch)
                .metadata(metadata)
                .payload(payload)
                .build());
    }

    private static byte[] readBytes(ByteBuf in, int size) {
        if (size == 0) {
            return new byte[0];
        }
        byte[] out = new byte[size];
        in.readBytes(out);
        return out;
    }

    private static byte[] bytes(byte[] in) {
        return in == null ? new byte[0] : in;
    }
}
