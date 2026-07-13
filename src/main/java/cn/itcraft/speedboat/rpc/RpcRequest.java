package cn.itcraft.speedboat.rpc;

import java.util.UUID;

public abstract class RpcRequest {
    
    private final String requestId;
    
    protected RpcRequest() {
        this.requestId = UUID.randomUUID().toString();
    }
    
    protected RpcRequest(String requestId) {
        this.requestId = requestId != null ? requestId : UUID.randomUUID().toString();
    }
    
    public String getRequestId() {
        return requestId;
    }
}
