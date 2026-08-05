package cn.itcraft.speedboat.rpc;

/**
 * RequestVoteResponse 类。
 * 
 * @author speedboat
 * @since 1.0.0
 */
public class RequestVoteResponse extends RpcResponse {

    private long term;
    private boolean voteGranted;

    public RequestVoteResponse() {
        super(null);
    }

    public RequestVoteResponse(String requestId, long term, boolean voteGranted) {
        super(requestId);
        this.term = term;
        this.voteGranted = voteGranted;
    }
    
    public RequestVoteResponse(long term, boolean voteGranted) {
        super(null);
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public long getTerm() {
        return term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }
}