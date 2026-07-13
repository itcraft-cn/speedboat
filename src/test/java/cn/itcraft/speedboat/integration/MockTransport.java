package cn.itcraft.speedboat.integration;

import cn.itcraft.speedboat.rpc.AppendEntriesRequest;
import cn.itcraft.speedboat.rpc.AppendEntriesResponse;
import cn.itcraft.speedboat.rpc.RequestVoteRequest;
import cn.itcraft.speedboat.rpc.RequestVoteResponse;
import cn.itcraft.speedboat.rpc.HeartbeatRequest;
import cn.itcraft.speedboat.rpc.HeartbeatResponse;
import cn.itcraft.speedboat.transport.TransportLayer;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class MockTransport implements TransportLayer {

    private final Map<String, MockTransport> nodeTransports = new ConcurrentHashMap<>();
    private RequestVoteHandler requestVoteHandler;
    private HeartbeatHandler heartbeatHandler;
    private AppendEntriesHandler appendEntriesHandler;

    public void registerNode(String nodeId, MockTransport transport) {
        nodeTransports.put(nodeId, transport);
    }

    public void unregisterNode(String nodeId) {
        nodeTransports.remove(nodeId);
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
        return CompletableFuture.completedFuture(new HeartbeatResponse(request.getTerm(), false));
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> sendAppendEntries(String peerId, AppendEntriesRequest request) {
        MockTransport peerTransport = nodeTransports.get(peerId);
        if (peerTransport != null && peerTransport.getAppendEntriesHandler() != null) {
            AppendEntriesResponse response = peerTransport.getAppendEntriesHandler().handle(request);
            return CompletableFuture.completedFuture(response);
        }
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
}