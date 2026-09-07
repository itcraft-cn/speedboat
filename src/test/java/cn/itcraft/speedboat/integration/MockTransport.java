package cn.itcraft.speedboat.integration;

import cn.itcraft.speedboat.rpc.AppendEntriesRequest;
import cn.itcraft.speedboat.rpc.AppendEntriesResponse;
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
            RequestVoteResponse response = peerTransport.getRequestVoteHandler().handle(request);
            return CompletableFuture.completedFuture(response);
        }
        return CompletableFuture.completedFuture(new RequestVoteResponse(request.getTerm(), false));
    }

    @Override
    public CompletableFuture<HeartbeatResponse> sendHeartbeat(String peerId, HeartbeatRequest request) {
        MockTransport peerTransport = nodeTransports.get(peerId);
        if (peerTransport != null && peerTransport.getHeartbeatHandler() != null) {
            HeartbeatResponse response = peerTransport.getHeartbeatHandler().handle(request);
            return CompletableFuture.completedFuture(response);
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
            AppendEntriesResponse response = peerTransport.getAppendEntriesHandler().handle(request);
            return CompletableFuture.completedFuture(response);
        }
        
        // 如果没有 handler，尝试通过反射调用 peer 的 RaftNode.handleAppendEntries
        logger.info("MockTransport.sendAppendEntries: peerId={}, nodeId={}, nodeRaftNodes={}, nodeTransports={}", 
            peerId, nodeId, nodeRaftNodes.keySet(), nodeTransports.keySet());
        Object raftNode = nodeRaftNodes.get(peerId);
        if (raftNode != null) {
            try {
                Method method = raftNode.getClass().getMethod("handleAppendEntries", AppendEntriesRequest.class);
                AppendEntriesResponse response = (AppendEntriesResponse) method.invoke(raftNode, request);
                logger.info("MockTransport.reflective call: peerId={}, nodeId={}, requestTerm={}, responseSuccess={}, responseMatchIndex={}", 
                    peerId, nodeId, request.getTerm(), response.isSuccess(), response.getMatchIndex());
                return CompletableFuture.completedFuture(response);
            } catch (Exception e) {
                System.err.println("MockTransport.sendAppendEntries反射调用失败: peerId=" + peerId + ", nodeId=" + nodeId + ", error=" + e.getClass().getName());
                e.printStackTrace();
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
        heartbeatHandler = null;
        appendEntriesHandler = null;
    }
    
    public void registerRaftNode(String nodeId, Object raftNode) {
        nodeRaftNodes.put(nodeId, raftNode);
    }
}