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

    /**
     * 优先级表版本号（阶段五人工升级 API 传播载体）。
     *
     * <p>父组 Leader 随心跳携带当前优先级快照的 epoch；接收方仅当该值 &gt; 本表 epoch
     * 才采纳其权重，构成"只升不降"的单调收敛（主机房重连后以低 term 退让并学到新表）。
     * 0 表示不携带（子组/单机房/旧版本节点），接收方忽略。</p>
     *
     * <p><b>注意事项：</b>本字段与 {@link #priorityWeights} 必须追加在类的<b>末尾</b>。
     * Protostuff 的 {@code RuntimeSchema} 按字段<b>声明顺序</b>分配 field ID，
     * 追加到末尾可保持既有线格式兼容（旧版本节点收到后为缺省值）。</p>
     */
    private long priorityEpoch;
    /** 优先级权重表编码（"dc=w,dc=w"；空表示不携带）。与 {@link #priorityEpoch} 成对追加在类末尾。 */
    private String priorityWeights;

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

    /** 优先级表版本号（0 表示不携带） */
    public long getPriorityEpoch() {
        return priorityEpoch;
    }

    /** 优先级权重表编码（"dc=w,dc=w"；空表示不携带） */
    public String getPriorityWeights() {
        return priorityWeights;
    }

    /**
     * 携带优先级传播快照（父组 Leader 心跳用）。
     *
     * @param priorityEpoch  优先级表版本号（&gt; 本表 epoch 才被接收方采纳）
     * @param priorityWeights 权重表编码（"dc=w,dc=w"）；可为 null
     */
    public void setPrioritySnapshot(long priorityEpoch, String priorityWeights) {
        this.priorityEpoch = priorityEpoch;
        this.priorityWeights = priorityWeights;
    }

    public static AppendEntriesRequest heartbeat(long term, String leaderId, 
                                                   long prevLogIndex, long prevLogTerm, 
                                                   long leaderCommit) {
        return new AppendEntriesRequest(term, leaderId, prevLogIndex, prevLogTerm, 
                                         Collections.emptyList(), leaderCommit);
    }
}