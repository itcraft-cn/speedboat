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
import io.netty.util.concurrent.GlobalEventExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

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
                        .addLast(new ByteArrayDecoder())
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
                        .addLast(new ByteArrayDecoder())
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
        for (String nodeId : channels.keySet()) {
            sendRequestVote(nodeId, request);
        }
    }
    
    public List<CompletableFuture<HeartbeatResponse>> broadcastHeartbeat(HeartbeatRequest request) {
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
