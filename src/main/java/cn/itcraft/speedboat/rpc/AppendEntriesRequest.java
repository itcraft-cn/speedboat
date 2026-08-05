package cn.itcraft.speedboat.rpc;

import cn.itcraft.speedboat.raft.LogEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * AppendEntriesRequest 类。
 * 
 * @author speedboat
 * @since 1.0.0
 */
public class AppendEntriesRequest extends RpcRequest {

    private long term;
    private String leaderId;
    private long prevLogIndex;
    private long prevLogTerm;
    private List<LogEntry> entries;
    private long leaderCommit;

    public AppendEntriesRequest() {
        super();
        this.entries = new ArrayList<>();
    }

    public AppendEntriesRequest(long term, String leaderId, long prevLogIndex, 
                                 long prevLogTerm, List<LogEntry> entries, long leaderCommit) {
        super();
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries != null ? new ArrayList<>(entries) : new ArrayList<>();
        this.leaderCommit = leaderCommit;
    }

    public long getTerm() {
        return term;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public long getPrevLogIndex() {
        return prevLogIndex;
    }

    public long getPrevLogTerm() {
        return prevLogTerm;
    }

    public List<LogEntry> getEntries() {
        return entries != null ? Collections.unmodifiableList(entries) : Collections.emptyList();
    }

    public long getLeaderCommit() {
        return leaderCommit;
    }

    public static AppendEntriesRequest heartbeat(long term, String leaderId, 
                                                   long prevLogIndex, long prevLogTerm, 
                                                   long leaderCommit) {
        return new AppendEntriesRequest(term, leaderId, prevLogIndex, prevLogTerm, 
                                         Collections.emptyList(), leaderCommit);
    }
}