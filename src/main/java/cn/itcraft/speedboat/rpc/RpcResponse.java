package cn.itcraft.speedboat.rpc;

/**
 * RPC 响应抽象基类，定义所有 RPC 响应的公共属性和行为。
 * 
 * <p>包含请求 ID 匹配逻辑，用于关联请求和响应。</p>
 * 
 * @author speedboat
 * @since 1.0.0
 */
public abstract class RpcResponse {
    
    private final String requestId;
    
    protected RpcResponse(String requestId) {
        this.requestId = requestId;
    }
    
    public String getRequestId() {
        return requestId;
    }
}