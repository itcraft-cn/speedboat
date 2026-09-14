package cn.itcraft.speedboat.persistence;

/**
 * 持久化任期状态记录（term + votedFor + leaderId 的恢复快照）。
 *
 * <p>对应 {@link RaftStore#restoreTerm()} 的返回值——
 * 节点重启时据此重建 term/votedFor/leader 视角的一等快照。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
public class RaftTermRecord {

    private long term;
    private String votedFor;
    private String leaderId;

    public RaftTermRecord() {
    }

    public RaftTermRecord(long term, String votedFor, String leaderId) {
        this.term = term;
        this.votedFor = votedFor;
        this.leaderId = leaderId;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public String getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(String votedFor) {
        this.votedFor = votedFor;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }
}
