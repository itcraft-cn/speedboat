package cn.itcraft.speedboat.rpc;

/**
 * RequestVoteRequest 类。
 * 
 * @author speedboat
 * @since 1.0.0
 */
public class RequestVoteRequest extends RpcRequest {

    private long term;
    private String candidateId;
    private int voteWeight;

    public RequestVoteRequest() {
        super();
    }

    public RequestVoteRequest(long term, String candidateId, int voteWeight) {
        super();
        this.term = term;
        this.candidateId = candidateId;
        this.voteWeight = voteWeight;
    }

    public long getTerm() {
        return term;
    }

    public String getCandidateId() {
        return candidateId;
    }

    public int getVoteWeight() {
        return voteWeight;
    }
}