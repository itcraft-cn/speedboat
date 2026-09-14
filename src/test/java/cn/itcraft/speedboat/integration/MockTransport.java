package cn.itcraft.speedboat.integration;

import cn.itcraft.speedboat.rpc.AppendEntriesRequest;
import cn.itcraft.speedboat.rpc.AppendEntriesResponse;
import cn.itcraft.speedboat.rpc.PreVoteRequest;
import cn.itcraft.speedboat.rpc.PreVoteResponse;
import cn.itcraft.speedboat.rpc.RequestVoteRequest;
import cn.itcraft.speedboat.rpc.RequestVoteResponse;
import cn.itcraft.speedboat.rpc.HeartbeatRequest;
import cn.itcraft.speedboat.rpc.HeartbeatResponse;
import cn.itcraft.speedboat.transport.TransportLayer;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockTransport implements TransportLayer {

    private static final Logger logger = LoggerFactory.getLogger(MockTransport.class);

    private final String nodeId;
    private final Map<String, MockTransport> nodeTransports = new ConcurrentHashMap<>();
    private final Map<String, Object> nodeRaftNodes = new ConcurrentHashMap<>();
    private RequestVoteHandler requestVoteHandler;
    private HeartbeatHandler heartbeatHandler;
    private AppendEntriesHandler appendEntriesHandler;
    private PreVoteHandler preVoteHandler;

    public MockTransport() {
        this.nodeId = null;
    }
    
    public MockTransport(String nodeId) {
        this.nodeId = nodeId;
    }
    
    public String getNodeId() {
        return nodeId;
    }

    public void registerNode(String nodeId, MockTransport transport) {
        nodeTransports.put(nodeId, transport);
    }

    public void unregisterNode(String nodeId) {
        nodeTransports.remove(nodeId);
        nodeRaftNodes.remove(nodeId);
    }

    @Override
    public CompletableFuture<RequestVoteResponse> sendRequestVote(String peerId, RequestVoteRequest request) {
        MockTransport peerTransport = nodeTransports.get(peerId);
        if (peerTransport != null && peerTransport.getRequestVoteHandler() != null) {
            // 异步转发：不阻塞发送方的调用线程（raft 单线程化后的死锁规避）
            return peerTransport.getRequestVoteHandler().handle(request).thenApply(r -> r);
        }
        return CompletableFuture.completedFuture(new RequestVoteResponse(request.getTerm(), false));
    }

    @Override
    public CompletableFuture<HeartbeatResponse> sendHeartbeat(String peerId, HeartbeatRequest request) {
        MockTransport peerTransport = nodeTransports.get(peerId);
        if (peerTransport != null && peerTransport.getHeartbeatHandler() != null) {
            return peerTransport.getHeartbeatHandler().handle(request);
        }
        
        // 如果没有 handler，尝试通过反射调用 peer 的 RaftNode.handleHeartbeat
        Object raftNode = nodeRaftNodes.get(peerId);
        if (raftNode != null) {
            try {
                Method method = raftNode.getClass().getMethod("handleHeartbeat", HeartbeatRequest.class);
                HeartbeatResponse response = (HeartbeatResponse) method.invoke(raftNode, request);
                return CompletableFuture.completedFuture(response);
            } catch (Exception e) {
                return CompletableFuture.completedFuture(new HeartbeatResponse(request.getTerm(), false));
            }
        }
        
        return CompletableFuture.completedFuture(new HeartbeatResponse(request.getTerm(), false));
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> sendAppendEntries(String peerId, AppendEntriesRequest request) {
        MockTransport peerTransport = nodeTransports.get(peerId);
        if (peerTransport != null && peerTransport.getAppendEntriesHandler() != null) {
            return peerTransport.getAppendEntriesHandler().handle(request);
        }
        
        // 如果没有 handler，尝试通过反射调用 peer 的 RaftNode.handleAppendEntries
        logger.debug("MockTransport.sendAppendEntries: peerId={}, nodeId={}", peerId, nodeId);
        Object raftNode = nodeRaftNodes.get(peerId);
        if (raftNode != null) {
            try {
                Method method = raftNode.getClass().getMethod("handleAppendEntries", AppendEntriesRequest.class);
                AppendEntriesResponse response = (AppendEntriesResponse) method.invoke(raftNode, request);
                logger.debug("MockTransport.reflective call: peerId={}, nodeId={}, requestTerm={}, responseSuccess={}, responseMatchIndex={}",
                    peerId, nodeId, request.getTerm(), response.isSuccess(), response.getMatchIndex());
                return CompletableFuture.completedFuture(response);
            } catch (Exception e) {
                logger.error("MockTransport.sendAppendEntries反射调用失败: peerId={}, nodeId={}, error={}",
                    peerId, nodeId, e.getClass().getName(), e);
                return CompletableFuture.completedFuture(new AppendEntriesResponse(request.getTerm(), false, 0));
            }
        }
        
        logger.warn("MockTransport.sendAppendEntries: raftNode not found for nodeId={}", nodeId);
        return CompletableFuture.completedFuture(new AppendEntriesResponse(request.getTerm(), false, 0));
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
    public void setAppendEntriesHandler(AppendEntriesHandler handler) {
        this.appendEntriesHandler = handler;
    }

    @Override
    public void setPreVoteHandler(PreVoteHandler handler) {
        this.preVoteHandler = handler;
    }

    public PreVoteHandler getPreVoteHandler() {
        return preVoteHandler;
    }

    @Override
    public CompletableFuture<PreVoteResponse> sendPreVote(String peerId, PreVoteRequest request) {
        MockTransport peerTransport = nodeTransports.get(peerId);
        if (peerTransport != null && peerTransport.getPreVoteHandler() != null) {
            return peerTransport.getPreVoteHandler().handle(request);
        }
        return CompletableFuture.completedFuture(new PreVoteResponse(request.getRequestId(), 0, false));
    }

    public RequestVoteHandler getRequestVoteHandler() {
        return requestVoteHandler;
    }



    public HeartbeatHandler getHeartbeatHandler() {
        return heartbeatHandler;
    }

    public AppendEntriesHandler getAppendEntriesHandler() {
        return appendEntriesHandler;
    }

    public void clear() {
        nodeTransports.clear();
        requestVoteHandler = null;
        preVoteHandler = null;
        heartbeatHandler = null;
        appendEntriesHandler = null;
    }
    
    public void registerRaftNode(String nodeId, Object raftNode) {
        nodeRaftNodes.put(nodeId, raftNode);
    }
}