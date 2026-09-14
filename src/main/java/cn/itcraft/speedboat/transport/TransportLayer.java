package cn.itcraft.speedboat.transport;

import cn.itcraft.speedboat.rpc.AppendEntriesRequest;
import cn.itcraft.speedboat.rpc.AppendEntriesResponse;
import cn.itcraft.speedboat.rpc.RequestVoteRequest;
import cn.itcraft.speedboat.rpc.RequestVoteResponse;
import cn.itcraft.speedboat.rpc.HeartbeatRequest;
import cn.itcraft.speedboat.rpc.HeartbeatResponse;
import cn.itcraft.speedboat.rpc.PreVoteRequest;
import cn.itcraft.speedboat.rpc.PreVoteResponse;

import java.util.concurrent.CompletableFuture;

/**
 * 网络传输层抽象接口，定义 Raft 节点间通信的标准契约。
 * 
 * <p>TransportLayer 是 Raft 实现网络隔离的核心抽象，允许不同的网络实现（Netty、gRPC、Mock 等）
 * 透明替换，便于测试和扩展。</p>
 * 
 * <p>核心职责：</p>
 * <ul>
 *   <li><b>RPC 发送</b>：异步发送三种 Raft RPC 消息（选举投票、心跳、日志复制）</li>
 *   <li><b>请求处理</b>：注册处理器来处理接收到的 RPC 请求</li>
 *   <li><b>连接管理</b>：管理到对等节点的网络连接（连接、断开、重连）</li>
 *   <li><b>序列化/反序列化</b>：将 Java 对象转换为网络字节流（由具体实现负责）</li>
 * </ul>
 * 
 * <p>接口设计原则：</p>
 * <ol>
 *   <li><b>异步非阻塞</b>：所有发送方法返回 {@link CompletableFuture}，支持异步回调</li>
 *   <li><b>类型安全</b>：每种 RPC 消息有独立的发送方法和处理器接口</li>
 *   <li><b>依赖倒置</b>：{@link RaftNode} 依赖 TransportLayer 抽象，而非具体实现</li>
 *   <li><b>单一职责</b>：只关注网络通信，不包含 Raft 业务逻辑</li>
 * </ol>
 * 
 * <p>实现要求：</p>
 * <ul>
 *   <li>必须保证消息的可靠传输（TCP 级别可靠性）</li>
 *   <li>必须处理网络超时和异常，返回适当的 {@link CompletableFuture} 异常</li>
 *   <li>应该支持连接池和连接复用，避免频繁创建连接的开销</li>
 *   <li>应该支持请求-响应匹配，确保响应与请求正确关联</li>
 * </ul>
 * 
 * <p>典型实现：</p>
 * <ul>
 *   <li>{@link NettyTransport}：基于 Netty 4.x 的生产级实现</li>
 *   <li>MockTransport：用于单元测试的模拟实现</li>
 *   <li>gRPCTransport：基于 gRPC 的跨语言实现（未来扩展）</li>
 * </ul>
 * 
 * <p>使用模式：</p>
 * <pre>{@code
 * // 创建传输层实例
 * TransportLayer transport = new NettyTransport(localEndpoint, peerEndpoints, serializer);
 * 
 * // 注册处理器
 * transport.setRequestVoteHandler(request -> {
 *     // 处理选举请求
 *     return new RequestVoteResponse(true, currentTerm);
 * });
 * 
 * // 发送异步请求
 * transport.sendRequestVote("node-2", request)
 *     .thenAccept(response -> {
 *         // 处理响应
 *     })
 *     .exceptionally(ex -> {
 *         // 处理网络异常
 *         return null;
 *     });
 * }</pre>
 * 
 * @author speedboat
 * @see NettyTransport
 * @see RpcMessageHandler
 * @since 1.0.0
 */
public interface TransportLayer {
    
    CompletableFuture<RequestVoteResponse> sendRequestVote(String peerId, RequestVoteRequest request);
    
    CompletableFuture<HeartbeatResponse> sendHeartbeat(String peerId, HeartbeatRequest request);
    
    CompletableFuture<AppendEntriesResponse> sendAppendEntries(String peerId, AppendEntriesRequest request);
    
    /**
     * 发送预投票探测请求（Phase C 预投票协议）。
     */
    CompletableFuture<PreVoteResponse> sendPreVote(String peerId, PreVoteRequest request);
    
    void setRequestVoteHandler(RequestVoteHandler handler);
    
    void setHeartbeatHandler(HeartbeatHandler handler);
    
    void setAppendEntriesHandler(AppendEntriesHandler handler);
    
    /** 注册预投票探测处理器（异步契约同 AppendEntries） */
    void setPreVoteHandler(PreVoteHandler handler);
    
    /**
     * 入站请求处理器契约（异步版）。
     *
     * <p>返回 CompletableFuture 而非同步结果，使请求的"入队"与"应答写回"
     * 解耦：调用方（通常是发送方的执行线程）绝不等待接收方的业务线程——
     * 否则 mock 集群/进程内通信会形成跨节点 raft 线程互等死锁。
     * 实现方应在内部将请求投递到自己的单线程执行器，并把最终应答
     * 写入返回的 Future（Netty 侧完成后异步回包）。</p>
     */
    @FunctionalInterface
    interface RequestVoteHandler {
        CompletableFuture<RequestVoteResponse> handle(RequestVoteRequest request);
    }
    
    @FunctionalInterface
    interface HeartbeatHandler {
        CompletableFuture<HeartbeatResponse> handle(HeartbeatRequest request);
    }
    
    @FunctionalInterface
    interface AppendEntriesHandler {
        CompletableFuture<AppendEntriesResponse> handle(AppendEntriesRequest request);
    }
    
    /**
     * 预投票探测处理器：sticky 判定（现任 leader 心跳新鲜则拒绝）在接收侧
     * raft 单线程内完成，结果经 Future 异步回写。
     */
    @FunctionalInterface
    interface PreVoteHandler {
        CompletableFuture<PreVoteResponse> handle(PreVoteRequest request);
    }
}