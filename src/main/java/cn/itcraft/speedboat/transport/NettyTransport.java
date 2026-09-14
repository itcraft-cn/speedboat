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
    /** 帧最大字节数：批量 AppendEntries 与成员消息可能聚合多条，1MB 安全上限（fix P1-5） */
    private static final int MAX_FRAME_LENGTH = 1024 * 1024;

    private final NodeEndpoint localEndpoint;
    private final List<NodeEndpoint> peers;
    private final CustomSerializer serializer;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    /** 每个 peer 维护一个 Channel，null 表示未连接或需要重连 */
    private final ConcurrentHashMap<String, Channel> channels;
    private final ChannelGroup channelGroup;
    /** 进行中的异步建连去重：同一 peer 只允许一条在途 connect */
    private final ConcurrentHashMap<String, CompletableFuture<Channel>> connectFutures = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    private final Map<String, CompletableFuture<RpcResponse>> pendingRequests = new ConcurrentHashMap<>();

    private RequestVoteHandler requestVoteHandler;
    private HeartbeatHandler heartbeatHandler;
    private AppendEntriesHandler appendEntriesHandler;
    private PreVoteHandler preVoteHandler;

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
                        .addLast(new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4))
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

    // ==================== 连接管理（异步版，借鉴 ABChecker） ====================

    /**
     * 懒获取连接（异步版）：有活跃 Channel 直接用；否则异步建连并在
     * 连接成功回调中写库。绝不阻塞调用线程——Raft 单线程（SC）与
     * Netty IO 线程（MP）都可能调用此方法，阻塞会卡死 raft 心跳。
     *
     * <p>同时进行的重复建连请求通过 {@code connectFutures} 去重，
     * 多个发送方共享同一个在途 connect。</p>
     */
    private CompletableFuture<Channel> getOrConnect(String targetNodeId) {
        Channel active = channels.get(targetNodeId);
        if (active != null && active.isActive()) {
            return CompletableFuture.completedFuture(active);
        }
        if (active != null) {
            logger.info("Channel for {} stale: active={}, open={}, registered={}",
                targetNodeId, active.isActive(), active.isOpen(), active.isRegistered());
            channels.remove(targetNodeId);
        }

        CompletableFuture<Channel> future = new CompletableFuture<>();
        CompletableFuture<Channel> inFlight = connectFutures.putIfAbsent(targetNodeId, future);
        if (inFlight != null) {
            return inFlight;
        }

        NodeEndpoint ep = findEndpoint(targetNodeId);
        if (ep == null) {
            connectFutures.remove(targetNodeId, future);
            future.completeExceptionally(new IllegalArgumentException("No endpoint found for " + targetNodeId));
            return future;
        }

        Bootstrap b = new Bootstrap();
        b.group(workerGroup)
            .channel(NioSocketChannel.class)
            // 建连超时交给 Netty option，替代原阻塞 await——防止卡死调用线程
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MS)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline()
                        .addLast(new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4))
                        .addLast(new ByteArrayEncoder())
                        .addLast(new RpcResponseHandler(NettyTransport.this, serializer));
                }
            });

        b.connect(ep.getHost(), ep.getPort()).addListener((ChannelFuture f) -> {
            connectFutures.remove(targetNodeId, future);
            if (f.isSuccess()) {
                Channel channel = f.channel();
                channels.put(targetNodeId, channel);
                channelGroup.add(channel);
                logger.info("Connected to {} at {}:{}", targetNodeId, ep.getHost(), ep.getPort());
                future.complete(channel);
            } else {
                logger.info("Connect to {} failed: {}", targetNodeId, f.cause() != null ? f.cause().getMessage() : "unknown");
                future.completeExceptionally(f.cause());
            }
        });
        return future;
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
        CompletableFuture<RequestVoteResponse> future = new CompletableFuture<>();
        pendingRequests.put(request.getRequestId(), (CompletableFuture<RpcResponse>) (CompletableFuture<?>) future);
        getOrConnect(targetNodeId)
            .thenAccept(ch -> writeRequest(ch, request, request.getRequestId(), future))
            .exceptionally(ex -> { failFast(request.getRequestId(), request, future); return null; });
        return future;
    }

    @Override
    public CompletableFuture<HeartbeatResponse> sendHeartbeat(String targetNodeId, HeartbeatRequest request) {
        CompletableFuture<HeartbeatResponse> future = new CompletableFuture<>();
        pendingRequests.put(request.getRequestId(), (CompletableFuture<RpcResponse>) (CompletableFuture<?>) future);
        getOrConnect(targetNodeId)
            .thenAccept(ch -> writeRequest(ch, request, request.getRequestId(), future))
            .exceptionally(ex -> { failFast(request.getRequestId(), request, future); return null; });
        return future;
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> sendAppendEntries(String targetNodeId, AppendEntriesRequest request) {
        CompletableFuture<AppendEntriesResponse> future = new CompletableFuture<>();
        pendingRequests.put(request.getRequestId(), (CompletableFuture<RpcResponse>) (CompletableFuture<?>) future);
        getOrConnect(targetNodeId)
            .thenAccept(ch -> writeRequest(ch, request, request.getRequestId(), future))
            .exceptionally(ex -> { failFast(request.getRequestId(), request, future); return null; });
        return future;
    }

    /**
     * 写请求帧并登记超时兜底。写失败同样折叠为"无响应"失败，
     * 符合 Transport 契约：传输层不参与共识语义，失败由算法侧超时兜底。
     */
    @SuppressWarnings("unchecked")
    private void writeRequest(Channel ch, Object request, String requestId, CompletableFuture<? extends RpcResponse> future) {
        try {
            ch.writeAndFlush(serializer.wrap(request));
            scheduleResponseTimeout(requestId, request);
        } catch (Exception e) {
            failFast(requestId, request, future);
        }
    }

    /** 按请求类型登记超时兜底：超时后移除 pending 并以"无响应"占位完成 */
    private void scheduleResponseTimeout(String requestId, Object request) {
        if (request instanceof RequestVoteRequest) {
            scheduleRequestVoteTimeout(requestId);
        } else if (request instanceof PreVoteRequest) {
            schedulePreVoteTimeout(requestId);
        } else if (request instanceof HeartbeatRequest) {
            scheduleHeartbeatTimeout(requestId);
        } else {
            scheduleAppendEntriesTimeout(requestId);
        }
    }

    /**
     * 快速失败路径：移除 pending 并以"无响应"占位完成 future。
     * term=0 表示失败，语义与旧版一致。
     */
    @SuppressWarnings("unchecked")
    private RpcResponse failFast(String requestId, Object request, CompletableFuture<? extends RpcResponse> future) {
        pendingRequests.remove(requestId);
        RpcResponse failure = toFailureResponse(requestId, request);
        ((CompletableFuture<RpcResponse>) future).complete(failure);
        return null;
    }

    /** 按请求类型生成"无响应"失败占位回复（term=0 表示失败，语义与旧版一致） */
    private RpcResponse toFailureResponse(String requestId, Object request) {
        if (request instanceof RequestVoteRequest) {
            return new RequestVoteResponse(requestId, 0, false);
        } else if (request instanceof PreVoteRequest) {
            return new PreVoteResponse(requestId, 0, false);
        } else if (request instanceof HeartbeatRequest) {
            return new HeartbeatResponse(requestId, 0, false);
        } else {
            return new AppendEntriesResponse(requestId, 0, false, 0);
        }
    }

    private void scheduleRequestVoteTimeout(String requestId) {
        workerGroup.schedule(() -> {
            CompletableFuture<RpcResponse> pending = pendingRequests.remove(requestId);
            if (pending != null && !pending.isDone()) {
                pending.complete(new RequestVoteResponse(requestId, 0, false));
            }
        }, DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    private void schedulePreVoteTimeout(String requestId) {
        workerGroup.schedule(() -> {
            CompletableFuture<RpcResponse> pending = pendingRequests.remove(requestId);
            if (pending != null && !pending.isDone()) {
                pending.complete(new PreVoteResponse(requestId, 0, false));
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

    @Override
    public CompletableFuture<PreVoteResponse> sendPreVote(String targetNodeId, PreVoteRequest request) {
        CompletableFuture<PreVoteResponse> future = new CompletableFuture<>();
        pendingRequests.put(request.getRequestId(), (CompletableFuture<RpcResponse>) (CompletableFuture<?>) future);
        getOrConnect(targetNodeId)
            .thenAccept(ch -> writeRequest(ch, request, request.getRequestId(), future))
            .exceptionally(ex -> { failFast(request.getRequestId(), (Object) request, future); return null; });
        return future;
    }

    // ==================== Handler 注册 ====================

    @Override
    public void setRequestVoteHandler(RequestVoteHandler handler) { this.requestVoteHandler = handler; }
    @Override
    public void setHeartbeatHandler(HeartbeatHandler handler) { this.heartbeatHandler = handler; }
    @Override
    public void setAppendEntriesHandler(AppendEntriesHandler handler) { this.appendEntriesHandler = handler; }
    @Override
    public void setPreVoteHandler(PreVoteHandler handler) { this.preVoteHandler = handler; }

    PreVoteHandler getPreVoteHandler() { return preVoteHandler; }
    RequestVoteHandler getRequestVoteHandler() { return requestVoteHandler; }
    HeartbeatHandler getHeartbeatHandler() { return heartbeatHandler; }
    AppendEntriesHandler getAppendEntriesHandler() { return appendEntriesHandler; }
    Map<String, CompletableFuture<RpcResponse>> getPendingRequests() { return pendingRequests; }
}
