package cn.itcraft.speedboat.raft.report;

import cn.itcraft.speedboat.raft.NodeState;

import java.util.Collections;
import java.util.Map;

/**
 * Raft 节点状态快照报告（Phase E，对标 MicroRaft report/RaftNodeReport）。
 *
 * <p>不可变值对象：一次快照包含角色/任期/成员/follower 复制进度等
 * 全量算法状态视图，供监控/告警/诊断只读消费。快照在 raft 单线程上
 * 收集（数据一致性由单消费者保证），对外发布为只读副本。</p>
 *
 * @author speedboat
 * @see RaftNodeReportListener
 * @since 1.1.0
 */
public class RaftNodeReport {

    /** 报告触发原因（事件驱动 + 周期兜底双通道） */
    public enum ReportReason {
        /** 角色变更（当选/降级） */
        ROLE_CHANGE,
        /** 任期提升 */
        TERM_CHANGE,
        /** 成员变更 */
        GROUP_MEMBERS_CHANGE,
        /** 周期性兜底快照（默认 10s） */
        PERIODIC,
        /** 其它 API 触发 */
        API_CALL
    }

    private final String nodeId;
    private final NodeState role;
    private final long term;
    private final String votedFor;
    private final String leaderId;
    private final long commitIndex;
    private final long lastApplied;
    private final int lastLogIndex;
    /** 成员列表（只读） */
    private final java.util.List<String> members;
    /** follower matchIndex 快照（只读；非 leader 为空） */
    private final Map<String, Long> followerMatchIndices;

    public RaftNodeReport(String nodeId, NodeState role, long term, String votedFor, String leaderId,
                          long commitIndex, long lastApplied, int lastLogIndex,
                          java.util.List<String> members, Map<String, Long> followerMatchIndices) {
        this.nodeId = nodeId;
        this.role = role;
        this.term = term;
        this.votedFor = votedFor;
        this.leaderId = leaderId;
        this.commitIndex = commitIndex;
        this.lastApplied = lastApplied;
        this.lastLogIndex = lastLogIndex;
        this.members = Collections.unmodifiableList(new java.util.ArrayList<>(members));
        this.followerMatchIndices = Collections.unmodifiableMap(new java.util.HashMap<>(followerMatchIndices));
    }

    public String getNodeId() {
        return nodeId;
    }

    public NodeState getRole() {
        return role;
    }

    public long getTerm() {
        return term;
    }

    public String getVotedFor() {
        return votedFor;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public long getLastApplied() {
        return lastApplied;
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }

    public java.util.List<String> getMembers() {
        return members;
    }

    public Map<String, Long> getFollowerMatchIndices() {
        return followerMatchIndices;
    }

    @Override
    public String toString() {
        return "RaftNodeReport{node=" + nodeId + ", role=" + role + ", term=" + term
            + ", leader=" + leaderId + ", commit=" + commitIndex
            + ", applied=" + lastApplied + ", lastLog=" + lastLogIndex
            + ", members=" + members + ", followerMatch=" + followerMatchIndices + "}";
    }
}
