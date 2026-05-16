package io.github.lyonbrown4d.nespa;

import io.github.lyonbrown4d.nespa.internal.CacheWire;
import io.github.lyonbrown4d.nespa.internal.Frame;
import io.github.lyonbrown4d.nespa.internal.NespaFrameCodec;
import io.github.lyonbrown4d.nespa.internal.Protocol;
import io.github.lyonbrown4d.nespa.internal.ResponseHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Builder;

public class DirectCacheClient implements CacheClient {
    private final String host;
    private final int port;
    private final Duration timeout;
    private final EventLoopGroup eventLoopGroup;
    private final AtomicLong nextRequestId = new AtomicLong();

    @Builder
    public DirectCacheClient(String address, Duration timeout) {
        InetSocketAddress socketAddress = parseAddress(address);
        this.host = socketAddress.getHostString();
        this.port = socketAddress.getPort();
        this.timeout = timeout == null || timeout.isZero() || timeout.isNegative() ? Duration.ofSeconds(5) : timeout;
        this.eventLoopGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    }

    @Override
    public Record set(Key key, byte[] value, SetOptions options) throws IOException {
        SetOptions next = options == null ? SetOptions.builder().build() : options;
        Frame frame = send(Protocol.OP_CACHE_SET, 0, CacheWire.encodeSetRequest(key, next), copy(value));
        return CacheWire.decodeRecord(frame.getMetadata(), frame.getPayload());
    }

    @Override
    public Record get(Key key, GetOptions options) throws IOException {
        GetOptions next = options == null ? GetOptions.builder().build() : options;
        Frame frame = send(Protocol.OP_CACHE_GET, 0, CacheWire.encodeGetRequest(key, next), new byte[0]);
        return CacheWire.decodeRecord(frame.getMetadata(), frame.getPayload());
    }

    @Override
    public boolean delete(Key key, DeleteOptions options) throws IOException {
        DeleteOptions next = options == null ? DeleteOptions.builder().build() : options;
        Frame frame = send(Protocol.OP_CACHE_DELETE, 0, CacheWire.encodeDeleteRequest(key, next), new byte[0]);
        return CacheWire.decodeBoolean(frame.getMetadata());
    }

    @Override
    public boolean exists(Key key, GetOptions options) throws IOException {
        GetOptions next = options == null ? GetOptions.builder().build() : options;
        Frame frame = send(Protocol.OP_CACHE_EXISTS, 0, CacheWire.encodeExistsRequest(key, next), new byte[0]);
        return CacheWire.decodeBoolean(frame.getMetadata());
    }

    @Override
    public boolean touch(Key key, TouchOptions options) throws IOException {
        TouchOptions next = options == null ? TouchOptions.builder().build() : options;
        Frame frame = send(Protocol.OP_CACHE_TOUCH, 0, CacheWire.encodeTouchRequest(key, next), new byte[0]);
        return CacheWire.decodeBoolean(frame.getMetadata());
    }

    @Override
    public Record adjust(Key key, AdjustOptions options) throws IOException {
        AdjustOptions next = options == null ? AdjustOptions.builder().build() : options;
        Frame frame = send(Protocol.OP_CACHE_ADJUST, 0, CacheWire.encodeAdjustRequest(key, next), new byte[0]);
        return CacheWire.decodeRecord(frame.getMetadata(), frame.getPayload());
    }

    @Override
    public void close() {
        eventLoopGroup.shutdownGracefully();
    }

    private Frame send(int op, long routeEpoch, byte[] metadata, byte[] payload) throws IOException {
        long requestId = nextRequestId.incrementAndGet();
        Frame request = Frame.builder()
                .op(op)
                .requestId(requestId)
                .routeEpoch(routeEpoch)
                .metadata(metadata)
                .payload(payload)
                .build();

        CompletableFuture<Frame> response = new CompletableFuture<>();
        Channel channel = connect(response);
        try {
            channel.writeAndFlush(request).sync();
            Frame frame = await(response);
            if (frame.getRequestId() != requestId) {
                throw new IOException("cache frame request id mismatch: " + frame.getRequestId() + " != " + requestId);
            }
            if ((frame.getFlags() & Protocol.FLAG_ERROR) != 0) {
                throw CacheException.fromFrame(frame);
            }
            return frame;
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new IOException("write cache frame interrupted", err);
        } finally {
            channel.close();
        }
    }

    private Channel connect(CompletableFuture<Frame> response) throws IOException {
        int timeoutMillis = Math.toIntExact(timeout.toMillis());
        Bootstrap bootstrap = new Bootstrap()
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeoutMillis)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) {
                        channel.pipeline()
                                .addLast(new ReadTimeoutHandler(timeoutMillis, TimeUnit.MILLISECONDS))
                                .addLast(new WriteTimeoutHandler(timeoutMillis, TimeUnit.MILLISECONDS))
                                .addLast(new NespaFrameCodec())
                                .addLast(new ResponseHandler(response));
                    }
                });
        try {
            return bootstrap.connect(host, port).sync().channel();
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new IOException("connect cache tcp server interrupted", err);
        }
    }

    private Frame await(CompletableFuture<Frame> response) throws IOException {
        try {
            return response.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new IOException("read cache frame interrupted", err);
        } catch (ExecutionException err) {
            Throwable cause = err.getCause();
            if (cause instanceof IOException io) {
                throw io;
            }
            throw new IOException("read cache frame failed", cause);
        } catch (TimeoutException err) {
            throw new IOException("read cache frame timed out", err);
        }
    }

    private static InetSocketAddress parseAddress(String address) {
        if (address == null || address.isBlank()) {
            throw new IllegalArgumentException("address is required");
        }
        String trimmed = address.trim();
        try {
            URI uri = new URI(trimmed);
            if (uri.getHost() != null && uri.getPort() > 0) {
                return new InetSocketAddress(uri.getHost(), uri.getPort());
            }
        } catch (URISyntaxException ignored) {
        }
        int colon = trimmed.lastIndexOf(':');
        if (colon <= 0 || colon == trimmed.length() - 1) {
            throw new IllegalArgumentException("address must be host:port");
        }
        return new InetSocketAddress(trimmed.substring(0, colon), Integer.parseInt(trimmed.substring(colon + 1)));
    }

    private static byte[] copy(byte[] value) {
        if (value == null || value.length == 0) {
            return new byte[0];
        }
        return value.clone();
    }
}
