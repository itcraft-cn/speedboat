package cn.itcraft.speedboat.rpc;

import java.util.UUID;

/**
 * RPC 请求抽象基类，定义所有 RPC 请求的公共属性和行为。
 * 
 * <p>包含请求 ID 生成和管理逻辑。</p>
 * 
 * @author speedboat
 * @since 1.0.0
 */
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
