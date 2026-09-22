package cn.itcraft.speedboat.rpc;

/**
 * AppendEntriesResponse 类。
 * 
 * @author speedboat
 * @since 1.0.0
 */
public class AppendEntriesResponse extends RpcResponse {

    private long term;
    private boolean success;
    private long matchIndex;
    /**
     * 失败时的快速回退提示（SOFAJRaft/Ratis 式 expectedNextIndex）：
     * 接收方建议的"应该重发的下一条 index"。
     * 0 表示未提供（旧版本/成功路径），leader 将退回逐条回退语义。
     * 【不含】同索引不同 term 的分割日志保护——leader 侧仍强制不高于 current-1。
     */
    private long expectedNextIndex;

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

    /**
     * 带快速回退提示的完整构造（失败路径用）。
     * 语义：建议 leader 把 nextIndex 直接落到 {@code expectedNextIndex}；
     * leader 侧须自行钳制 ≤ current-1，避免分割日志下的探测循环。
     */
    public AppendEntriesResponse(long term, boolean success, long matchIndex, long expectedNextIndex) {
        super(null);
        this.term = term;
        this.success = success;
        this.matchIndex = matchIndex;
        this.expectedNextIndex = expectedNextIndex;
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

    /** 失败路径的快速回退提示；0=未提供（成功路径恒 0） */
    public long getExpectedNextIndex() {
        return expectedNextIndex;
    }
}