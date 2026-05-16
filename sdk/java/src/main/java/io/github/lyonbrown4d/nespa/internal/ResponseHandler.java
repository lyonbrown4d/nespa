package io.github.lyonbrown4d.nespa.internal;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.concurrent.CompletableFuture;

public class ResponseHandler extends SimpleChannelInboundHandler<Frame> {
    private final CompletableFuture<Frame> response;

    public ResponseHandler(CompletableFuture<Frame> response) {
        this.response = response;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Frame frame) {
        response.complete(frame);
        ctx.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        response.completeExceptionally(cause);
        ctx.close();
    }
}
