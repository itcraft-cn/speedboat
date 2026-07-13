package cn.itcraft.speedboat.rpc;

public class AppendEntriesResponse extends RpcResponse {

    private long term;
    private boolean success;
    private long matchIndex;

    public AppendEntriesResponse() {
        super(null);
    }

    public AppendEntriesResponse(String requestId, long term, boolean success, long matchIndex) {
        super(requestId);
        this.term = term;
        this.success = success;
        this.matchIndex = matchIndex;
    }
    
    public AppendEntriesResponse(long term, boolean success, long matchIndex) {
        super(null);
        this.term = term;
        this.success = success;
        this.matchIndex = matchIndex;
    }

    public long getTerm() {
        return term;
    }

    public boolean isSuccess() {
        return success;
    }

    public long getMatchIndex() {
        return matchIndex;
    }
}