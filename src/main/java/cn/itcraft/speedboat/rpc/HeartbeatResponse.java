package cn.itcraft.speedboat.rpc;

/**
 * Heartbeat RPC响应对象
 * 
 * 用于Raft协议中的心跳响应，节点对心跳请求作出回应。
 * 包含响应者任期和心跳是否成功的信息。
 */
public class HeartbeatResponse extends RpcResponse {

    private long term;
    private boolean success;

    public HeartbeatResponse() {
        super(null);
    }

    /**
     * 构造Heartbeat响应（带requestId）
     *
     * @param requestId 请求ID
     * @param term 响应者任期号
     * @param success 心跳是否成功
     */
    public HeartbeatResponse(String requestId, long term, boolean success) {
        super(requestId);
        this.term = term;
        this.success = success;
    }
    
    /**
     * 构造Heartbeat响应（无requestId，用于服务端主动发送）
     *
     * @param term 响应者任期号
     * @param success 心跳是否成功
     */
    public HeartbeatResponse(long term, boolean success) {
        super(null);
        this.term = term;
        this.success = success;
    }

    public long getTerm() {
        return term;
    }

    public boolean isSuccess() {
        return success;
    }
}