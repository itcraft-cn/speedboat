package cn.itcraft.speedboat.rpc;

/**
 * PreVoteResponse 类（Phase C 预投票协议消息）。
 *
 * <p> granted=true 表示接收方愿意在对方发起正式选举时投票；
 * term 携带接收方当前任期，供候选者对齐（若本地任期更高则放弃探测）。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
public class PreVoteResponse extends RpcResponse {

    private long term;
    private boolean voteGranted;

    public PreVoteResponse() {
        super(null);
    }

    public PreVoteResponse(String requestId, long term, boolean voteGranted) {
        super(requestId);
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
