package cn.itcraft.speedboat.transport;

import cn.itcraft.speedboat.rpc.*;
import cn.itcraft.speedboat.serialize.CustomSerializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.bytes.ByteArrayEncoder;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * 基于 Netty 的网络传输层。
 *
 * <p>连接管理借鉴 ABChecker 模式：懒连接 + 发送失败置空重建。</p>
 */
public class NettyTransport implements TransportLayer {

    private static final Logger logger = LoggerFactory.getLogger(NettyTransport.class);
    private static final long DEFAULT_TIMEOUT_MS = 5000;
    private static final int CONNECT_TIMEOUT_MS = 1000;

    private final NodeEndpoint localEndpoint;
    private final List<NodeEndpoint> peers;
    private final CustomSerializer serializer;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    /** 每个 peer 维护一个 Channel，null 表示未连接或需要重连 */
    private final ConcurrentHashMap<String, Channel> channels;
    private final ChannelGroup channelGroup;

    @SuppressWarnings("unchecked")
    private final Map<String, CompletableFuture<RpcResponse>> pendingRequests = new ConcurrentHashMap<>();

    private RequestVoteHandler requestVoteHandler;
    private HeartbeatHandler heartbeatHandler;
    private AppendEntriesHandler appendEntriesHandler;

    public NettyTransport(NodeEndpoint localEndpoint, List<NodeEndpoint> peers, CustomSerializer serializer) {
        this.localEndpoint = localEndpoint;
        this.peers = peers;
        this.serializer = serializer;
        this.bossGroup = new NioEventLoopGroup(1);
        this.workerGroup = new NioEventLoopGroup();
        this.channels = new ConcurrentHashMap<>();
        this.channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    }

    // ==================== 广播方法 ====================

    public void broadcastHeartbeat(HeartbeatRequest request) {
        for (NodeEndpoint peer : peers) {
            if (!peer.getNodeId().equals(localEndpoint.getNodeId())) {
                sendHeartbeat(peer.getNodeId(), request);
            }
        }
    }

    public void broadcastRequestVote(RequestVoteRequest request) {
        for (NodeEndpoint peer : peers) {
            if (!peer.getNodeId().equals(localEndpoint.getNodeId())) {
                sendRequestVote(peer.getNodeId(), request);
            }
        }
    }

    // ==================== 生命周期 ====================

    public void start() {
        ServerBootstrap sb = new ServerBootstrap();
        sb.group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline()
                        .addLast(new LengthFieldBasedFrameDecoder(1024, 0, 4, 0, 4))
                        .addLast(new ByteArrayEncoder())
                        .addLast(new RpcMessageHandler(NettyTransport.this, serializer));
                }
            });
        try {
            ChannelFuture f = sb.bind(localEndpoint.getPort()).sync();
            logger.info("Server bound to port {}", localEndpoint.getPort());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // 不做 connectToPeers，改为懒连接：首次发送时才连接
    }

    public void shutdown() {
        channels.values().forEach(Channel::close);
        channelGroup.close();
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
    }

    // ==================== 连接管理（借鉴 ABChecker） ====================

    /**
     * 懒获取连接：有则用，无则建；发送失败则置空，下次重建。
     *
     * <p>参考 ABCheckSockLoop.checkChannel() 的模式：</p>
     * <pre>
     *   if (channel == null) {
     *       channel = SocketChannel.open();
     *       channel.connect(otherHostPort);
     *   }
     * </pre>
     */
    private Channel getChannel(String targetNodeId) {
        Channel ch = channels.get(targetNodeId);
        if (ch != null && ch.isActive()) {
            return ch;
        }
        if (ch != null) {
            logger.info("Channel for {} stale: active={}, open={}, writable={}, registered={}", 
                targetNodeId, ch.isActive(), ch.isOpen(), ch.isWritable(), ch.isRegistered());
            channels.remove(targetNodeId);
        }
        return connectTo(targetNodeId);
    }

    /**
     * 建立到指定 peer 的连接。失败返回 null，下次发送时自动重试。
     */
    private synchronized Channel connectTo(String targetNodeId) {
        // 双重检查：可能其他线程已建立连接
        Channel existing = channels.get(targetNodeId);
        if (existing != null && existing.isActive()) {
            return existing;
        }

        NodeEndpoint ep = findEndpoint(targetNodeId);
        if (ep == null) {
            logger.warn("No endpoint found for {}", targetNodeId);
            return null;
        }

        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                            .addLast(new LengthFieldBasedFrameDecoder(1024, 0, 4, 0, 4))
                            .addLast(new ByteArrayEncoder())
                            .addLast(new RpcResponseHandler(NettyTransport.this, serializer));
                    }
                });

            ChannelFuture f = b.connect(ep.getHost(), ep.getPort());
            boolean ok = f.await(CONNECT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (ok && f.isSuccess()) {
                Channel channel = f.channel();
                channels.put(targetNodeId, channel);
                channelGroup.add(channel);
                logger.info("Connected to {} at {}:{}", targetNodeId, ep.getHost(), ep.getPort());
                return channel;
            }
        } catch (Exception e) {
            logger.debug("Connect to {} failed: {}", targetNodeId, e.getMessage());
        }
        return null;
    }

    /**
     * 发送失败时置空 channel，下次自动重连。
     */
    private void invalidateChannel(String targetNodeId) {
        channels.remove(targetNodeId);
    }

    private NodeEndpoint findEndpoint(String nodeId) {
        for (NodeEndpoint ep : peers) {
            if (ep.getNodeId().equals(nodeId)) {
                return ep;
            }
        }
        return null;
    }

    // ==================== RPC 发送 ====================

    @Override
    public CompletableFuture<RequestVoteResponse> sendRequestVote(String targetNodeId, RequestVoteRequest request) {
        Channel ch = getChannel(targetNodeId);
        if (ch == null) {
            return CompletableFuture.completedFuture(
                new RequestVoteResponse(request.getRequestId(), 0, false));
        }
        CompletableFuture<RequestVoteResponse> future = new CompletableFuture<>();
        pendingRequests.put(request.getRequestId(), (CompletableFuture<RpcResponse>) (CompletableFuture<?>) future);
        try {
            ch.writeAndFlush(serializer.wrap(request));
            scheduleRequestVoteTimeout(request.getRequestId());
        } catch (Exception e) {
            pendingRequests.remove(request.getRequestId());
            future.complete(new RequestVoteResponse(request.getRequestId(), 0, false));
        }
        return future;
    }

    @Override
    public CompletableFuture<HeartbeatResponse> sendHeartbeat(String targetNodeId, HeartbeatRequest request) {
        Channel ch = getChannel(targetNodeId);
        if (ch == null) {
            return CompletableFuture.completedFuture(
                new HeartbeatResponse(request.getRequestId(), 0, false));
        }
        CompletableFuture<HeartbeatResponse> future = new CompletableFuture<>();
        pendingRequests.put(request.getRequestId(), (CompletableFuture<RpcResponse>) (CompletableFuture<?>) future);
        try {
            ch.writeAndFlush(serializer.wrap(request));
            scheduleHeartbeatTimeout(request.getRequestId());
        } catch (Exception e) {
            pendingRequests.remove(request.getRequestId());
            future.complete(new HeartbeatResponse(request.getRequestId(), 0, false));
        }
        return future;
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> sendAppendEntries(String targetNodeId, AppendEntriesRequest request) {
        Channel ch = getChannel(targetNodeId);
        if (ch == null) {
            return CompletableFuture.completedFuture(
                new AppendEntriesResponse(request.getRequestId(), 0, false, 0));
        }
        CompletableFuture<AppendEntriesResponse> future = new CompletableFuture<>();
        pendingRequests.put(request.getRequestId(), (CompletableFuture<RpcResponse>) (CompletableFuture<?>) future);
        try {
            ch.writeAndFlush(serializer.wrap(request));
            scheduleAppendEntriesTimeout(request.getRequestId());
        } catch (Exception e) {
            pendingRequests.remove(request.getRequestId());
            future.complete(new AppendEntriesResponse(request.getRequestId(), 0, false, 0));
        }
        return future;
    }

    private void scheduleRequestVoteTimeout(String requestId) {
        workerGroup.schedule(() -> {
            CompletableFuture<RpcResponse> pending = pendingRequests.remove(requestId);
            if (pending != null && !pending.isDone()) {
                pending.complete(new RequestVoteResponse(requestId, 0, false));
            }
        }, DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    private void scheduleHeartbeatTimeout(String requestId) {
        workerGroup.schedule(() -> {
            CompletableFuture<RpcResponse> pending = pendingRequests.remove(requestId);
            if (pending != null && !pending.isDone()) {
                pending.complete(new HeartbeatResponse(requestId, 0, false));
            }
        }, DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    private void scheduleAppendEntriesTimeout(String requestId) {
        workerGroup.schedule(() -> {
            CompletableFuture<RpcResponse> pending = pendingRequests.remove(requestId);
            if (pending != null && !pending.isDone()) {
                pending.complete(new AppendEntriesResponse(requestId, 0, false, 0));
            }
        }, DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    // ==================== Handler 注册 ====================

    @Override
    public void setRequestVoteHandler(RequestVoteHandler handler) { this.requestVoteHandler = handler; }
    @Override
    public void setHeartbeatHandler(HeartbeatHandler handler) { this.heartbeatHandler = handler; }
    @Override
    public void setAppendEntriesHandler(AppendEntriesHandler handler) { this.appendEntriesHandler = handler; }

    RequestVoteHandler getRequestVoteHandler() { return requestVoteHandler; }
    HeartbeatHandler getHeartbeatHandler() { return heartbeatHandler; }
    AppendEntriesHandler getAppendEntriesHandler() { return appendEntriesHandler; }
    Map<String, CompletableFuture<RpcResponse>> getPendingRequests() { return pendingRequests; }
}
