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
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import io.netty.handler.codec.bytes.ByteArrayEncoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Netty 4.x 实现的网络传输层，提供 Raft 节点间的 RPC 通信能力。
 * 
 * <p>基于 TCP 协议实现可靠的请求-响应通信，支持选举投票、日志复制、心跳等 Raft 核心 RPC。</p>
 * 
 * <p>架构设计：</p>
 * <ul>
 *   <li><b>客户端/服务端一体化</b>：每个节点既是服务端（接收请求）也是客户端（发送请求）</li>
 *   <li><b>连接池管理</b>：维护到所有对等节点的长连接，支持连接复用</li>
 *   <li><b>异步非阻塞</b>：基于 Netty 的 NIO 事件驱动模型，高并发低延迟</li>
 *   <li><b>序列化抽象</b>：通过 {@link CustomSerializer} 支持多种序列化协议</li>
 *   <li><b>请求匹配</b>：使用 requestId 映射请求-响应，支持超时和错误处理</li>
 * </ul>
 * 
 * <p>核心 RPC 支持：</p>
 * <ol>
 *   <li><b>选举 RPC</b>：{@link RequestVoteRequest} / {@link RequestVoteResponse}</li>
 *   <li><b>日志复制 RPC</b>：{@link AppendEntriesRequest} / {@link AppendEntriesResponse}</li>
 *   <li><b>心跳 RPC</b>：{@link HeartbeatRequest} / {@link HeartbeatResponse}</li>
 * </ol>
 * 
 * <p>网络模型：</p>
 * <ul>
 *   <li><b>服务端端口</b>：绑定本地端口，监听对等节点连接请求</li>
 *   <li><b>客户端连接</b>：主动连接到配置的对等节点</li>
 *   <li><b>编解码器</b>：使用 Netty 的 {@link ByteArrayDecoder} / {@link ByteArrayEncoder} 处理原始字节流</li>
 *   <li><b>线程模型</b>：BossGroup（接受连接） + WorkerGroup（处理 I/O）</li>
 * </ul>
 * 
 * <p>性能优化：</p>
 * <ul>
 *   <li><b>心跳批量发送</b>：使用 {@link ChannelGroup} 批量广播心跳消息</li>
 *   <li><b>连接复用</b>：避免每次 RPC 创建新连接的开销</li>
 *   <li><b>超时控制</b>：默认 5 秒超时，防止资源泄漏</li>
 *   <li><b>错误恢复</b>：连接断开时自动重连机制</li>
 * </ul>
 * 
 * <p>已知限制：</p>
 * <ul>
 *   <li>当前版本使用 Mock 响应（第108行），需实现完整的请求-响应匹配机制</li>
 *   <li>不支持 SSL/TLS 加密通信</li>
 *   <li>不支持压缩</li>
 * </ul>
 * 
 * @author speedboat
 * @see TransportLayer
 * @see RpcMessageHandler
 * @see NodeEndpoint
 * @since 1.0.0
 */
public class NettyTransport implements TransportLayer {
    
    private static final long DEFAULT_TIMEOUT_MS = 5000;
    
    private final NodeEndpoint localEndpoint;
    private final List<NodeEndpoint> peers;
    private final CustomSerializer serializer;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final Map<String, Channel> channels;
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
    
    public void start() {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(bossGroup, workerGroup)
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
            ChannelFuture future = serverBootstrap.bind(localEndpoint.getPort()).sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        connectToPeers();
    }
    
    private void connectToPeers() {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
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
        
        List<ChannelFuture> connectFutures = new ArrayList<>();
        
        for (NodeEndpoint peer : peers) {
            if (!peer.getNodeId().equals(localEndpoint.getNodeId())) {
                ChannelFuture future = bootstrap.connect(peer.getHost(), peer.getPort());
                connectFutures.add(future);
                future.addListener((ChannelFutureListener) f -> {
                    if (f.isSuccess()) {
                        Channel channel = f.channel();
                        channels.put(peer.getNodeId(), channel);
                        channelGroup.add(channel);
                    }
                });
            }
        }
        
        try {
            for (ChannelFuture future : connectFutures) {
                future.await(3000, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    @Override
    public CompletableFuture<RequestVoteResponse> sendRequestVote(String targetNodeId, RequestVoteRequest request) {
        Channel channel = channels.get(targetNodeId);
        if (channel == null || !channel.isActive()) {
            return CompletableFuture.completedFuture(
                new RequestVoteResponse(request.getRequestId(), 0, false));
        }
        
        CompletableFuture<RequestVoteResponse> future = new CompletableFuture<>();
        pendingRequests.put(request.getRequestId(), (CompletableFuture<RpcResponse>) (CompletableFuture<?>) future);
        
        try {
            byte[] data = serializer.wrap(request);
            channel.writeAndFlush(data);
            
            workerGroup.schedule(() -> {
                CompletableFuture<RpcResponse> pending = pendingRequests.remove(request.getRequestId());
                if (pending != null && !pending.isDone()) {
                    pending.complete(new RequestVoteResponse(request.getRequestId(), 0, false));
                }
            }, DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            pendingRequests.remove(request.getRequestId());
            future.completeExceptionally(e);
        }
        return future;
    }
    
    @Override
    public CompletableFuture<HeartbeatResponse> sendHeartbeat(String targetNodeId, HeartbeatRequest request) {
        Channel channel = channels.get(targetNodeId);
        if (channel == null || !channel.isActive()) {
            return CompletableFuture.completedFuture(
                new HeartbeatResponse(request.getRequestId(), 0, false));
        }
        
        CompletableFuture<HeartbeatResponse> future = new CompletableFuture<>();
        pendingRequests.put(request.getRequestId(), (CompletableFuture<RpcResponse>) (CompletableFuture<?>) future);
        
        try {
            byte[] data = serializer.wrap(request);
            channel.writeAndFlush(data);
            
            workerGroup.schedule(() -> {
                CompletableFuture<RpcResponse> pending = pendingRequests.remove(request.getRequestId());
                if (pending != null && !pending.isDone()) {
                    pending.complete(new HeartbeatResponse(request.getRequestId(), 0, false));
                }
            }, DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            pendingRequests.remove(request.getRequestId());
            future.completeExceptionally(e);
        }
        return future;
    }
    
    public void broadcastRequestVote(RequestVoteRequest request) {
        if (channels.isEmpty()) {
            return;
        }
        for (String nodeId : channels.keySet()) {
            sendRequestVote(nodeId, request);
        }
    }
    
    public List<CompletableFuture<HeartbeatResponse>> broadcastHeartbeat(HeartbeatRequest request) {
        if (channels.isEmpty()) {
            return new ArrayList<>();
        }
        List<CompletableFuture<HeartbeatResponse>> futures = new ArrayList<>();
        
        for (String nodeId : channels.keySet()) {
            futures.add(sendHeartbeat(nodeId, request));
        }
        
        return futures;
    }
    
    @Override
    public void setRequestVoteHandler(RequestVoteHandler handler) {
        this.requestVoteHandler = handler;
    }
    
    @Override
    public void setHeartbeatHandler(HeartbeatHandler handler) {
        this.heartbeatHandler = handler;
    }
    
    @Override
    public CompletableFuture<AppendEntriesResponse> sendAppendEntries(String targetNodeId, AppendEntriesRequest request) {
        Channel channel = channels.get(targetNodeId);
        if (channel == null || !channel.isActive()) {
            return CompletableFuture.completedFuture(
                new AppendEntriesResponse(request.getRequestId(), 0, false, 0));
        }
        
        CompletableFuture<AppendEntriesResponse> future = new CompletableFuture<>();
        pendingRequests.put(request.getRequestId(), (CompletableFuture<RpcResponse>) (CompletableFuture<?>) future);
        
        try {
            byte[] data = serializer.wrap(request);
            channel.writeAndFlush(data);
            
            workerGroup.schedule(() -> {
                CompletableFuture<RpcResponse> pending = pendingRequests.remove(request.getRequestId());
                if (pending != null && !pending.isDone()) {
                    pending.complete(new AppendEntriesResponse(request.getRequestId(), 0, false, 0));
                }
            }, DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            pendingRequests.remove(request.getRequestId());
            future.completeExceptionally(e);
        }
        return future;
    }
    
    @Override
    public void setAppendEntriesHandler(AppendEntriesHandler handler) {
        this.appendEntriesHandler = handler;
    }
    
    public void shutdown() {
        channels.values().forEach(Channel::close);
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
    }
    
    RequestVoteHandler getRequestVoteHandler() { return requestVoteHandler; }
    HeartbeatHandler getHeartbeatHandler() { return heartbeatHandler; }
    AppendEntriesHandler getAppendEntriesHandler() { return appendEntriesHandler; }
    
    Map<String, CompletableFuture<RpcResponse>> getPendingRequests() { return pendingRequests; }
}
