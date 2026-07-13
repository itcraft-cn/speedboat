package cn.itcraft.speedboat.rpc;

public class HeartbeatRequest extends RpcRequest {

    private long term;
    private String leaderId;

    public HeartbeatRequest() {
        super();
    }

    public HeartbeatRequest(long term, String leaderId) {
        super();
        this.term = term;
        this.leaderId = leaderId;
    }

    public long getTerm() {
        return term;
    }

    public String getLeaderId() {
        return leaderId;
    }
}