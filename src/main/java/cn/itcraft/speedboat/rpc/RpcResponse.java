package cn.itcraft.speedboat.rpc;

public abstract class RpcResponse {
    
    private final String requestId;
    
    protected RpcResponse(String requestId) {
        this.requestId = requestId;
    }
    
    public String getRequestId() {
        return requestId;
    }
}