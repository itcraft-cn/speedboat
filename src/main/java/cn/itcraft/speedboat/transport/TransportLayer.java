package cn.itcraft.speedboat.transport;

import cn.itcraft.speedboat.rpc.AppendEntriesRequest;
import cn.itcraft.speedboat.rpc.AppendEntriesResponse;
import cn.itcraft.speedboat.rpc.RequestVoteRequest;
import cn.itcraft.speedboat.rpc.RequestVoteResponse;
import cn.itcraft.speedboat.rpc.HeartbeatRequest;
import cn.itcraft.speedboat.rpc.HeartbeatResponse;

import java.util.concurrent.CompletableFuture;

public interface TransportLayer {
    
    CompletableFuture<RequestVoteResponse> sendRequestVote(String peerId, RequestVoteRequest request);
    
    CompletableFuture<HeartbeatResponse> sendHeartbeat(String peerId, HeartbeatRequest request);
    
    CompletableFuture<AppendEntriesResponse> sendAppendEntries(String peerId, AppendEntriesRequest request);
    
    void setRequestVoteHandler(RequestVoteHandler handler);
    
    void setHeartbeatHandler(HeartbeatHandler handler);
    
    void setAppendEntriesHandler(AppendEntriesHandler handler);
    
    @FunctionalInterface
    interface RequestVoteHandler {
        RequestVoteResponse handle(RequestVoteRequest request);
    }
    
    @FunctionalInterface
    interface HeartbeatHandler {
        HeartbeatResponse handle(HeartbeatRequest request);
    }
    
    @FunctionalInterface
    interface AppendEntriesHandler {
        AppendEntriesResponse handle(AppendEntriesRequest request);
    }
}