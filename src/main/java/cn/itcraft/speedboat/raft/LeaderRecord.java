package cn.itcraft.speedboat.raft;

public class LeaderRecord {

    private final long term;
    private final String leaderId;

    public LeaderRecord(long term, String leaderId) {
        this.term = term;
        this.leaderId = leaderId;
    }

    public long getTerm() {
        return term;
    }

    public String getLeaderId() {
        return leaderId;
    }

    @Override
    public String toString() {
        return "LeaderRecord{term=" + term + ", leaderId='" + leaderId + "'}";
    }
}