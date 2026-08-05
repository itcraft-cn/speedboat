package cn.itcraft.speedboat.raft;

/**
 * Leader 记录，封装任期和 Leader 节点标识的不可变数据对象。
 * 
 * <p>用于在 Raft 集群中跟踪每个任期的 Leader 信息，支持选举状态恢复和集群监控。</p>
 * 
 * <p>主要用途：</p>
 * <ul>
 *   <li><b>选举历史</b>：记录每个任期的 Leader，便于故障诊断</li>
 *   <li><b>状态恢复</b>：节点重启后恢复已知的 Leader 信息</li>
 *   <li><b>监控指标</b>：统计 Leader 任期分布和切换频率</li>
 *   <li><b>日志匹配</b>：在日志复制中验证 Leader 身份</li>
 * </ul>
 * 
 * <p>不变性保证：</p>
 * <ul>
 *   <li>所有字段均为 final，线程安全</li>
 *   <li>hashCode/equals 基于 term 和 leaderId</li>
 *   <li>支持序列化传输</li>
 * </ul>
 * 
 * @author speedboat
 * @see Term
 * @see RaftNode
 * @since 1.0.0
 */
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