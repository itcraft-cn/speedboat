package cn.itcraft.speedboat.raft;

import java.util.Objects;

/**
 * 成员变更日志条目，用于在 Raft 集群中添加或移除节点。
 * 
 * <p>MemberChangeEntry 是 Raft 日志的一种特殊条目，封装了集群成员变更操作。
 * 通过 Raft 日志复制机制确保所有节点对成员变更达成共识。</p>
 * 
 * <p>变更类型：</p>
 * <ul>
 *   <li><b>ADD</b>：添加新节点到集群</li>
 *   <li><b>REMOVE</b>：从集群中移除现有节点</li>
 * </ul>
 * 
 * <p>变更流程：</p>
 * <ol>
 *   <li>Leader 接收成员变更请求</li>
 *   <li>创建 MemberChangeEntry 并追加到本地日志</li>
 *   <li>通过 AppendEntries RPC 复制到所有 Followers</li>
 *   <li>当日志提交后，更新集群成员配置</li>
 *   <li>新配置生效，后续 RPC 使用新成员列表</li>
 * </ol>
 * 
 * <p>安全性保证：</p>
 * <ul>
 *   <li>一次只能进行一个成员变更，防止配置混乱</li>
 *   <li>变更需要获得多数派确认才能提交</li>
 *   <li>旧配置和新配置的重叠期保证可用性</li>
 * </ul>
 * 
 * <p>使用示例：</p>
 * <pre>{@code
 * // 添加节点
 * MemberChangeEntry addEntry = new MemberChangeEntry(
 *     index, term, leaderId, 
 *     ChangeType.ADD, "node-4", "192.168.1.4:3000");
 * 
 * // 移除节点  
 * MemberChangeEntry removeEntry = new MemberChangeEntry(
 *     index, term, leaderId,
 *     ChangeType.REMOVE, "node-2", null);
 * }</pre>
 * 
 * @author speedboat
 * @see LogEntry
 * @see RaftNode#proposeAddMember(String)
 * @see RaftNode#proposeRemoveMember(String)
 * @since 1.0.0
 */
public class MemberChangeEntry extends LogEntry {

    public enum ChangeType {
        ADD,
        REMOVE
    }

    private final ChangeType changeType;
    private final String nodeId;
    private final String address;

    public MemberChangeEntry(long index, long term, String leaderId, ChangeType changeType, String nodeId, String address) {
        super(index, term, leaderId, EntryType.MEMBER_CHANGE);
        this.changeType = changeType;
        this.nodeId = nodeId;
        this.address = address;
    }

    public ChangeType getChangeType() {
        return changeType;
    }

    public String getNodeId() {
        return nodeId;
    }
    
    public String getPeerId() {
        return getNodeId();
    }

    public String getAddress() {
        return address;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        MemberChangeEntry that = (MemberChangeEntry) o;
        return changeType == that.changeType &&
                Objects.equals(nodeId, that.nodeId) &&
                Objects.equals(address, that.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), changeType, nodeId, address);
    }

    @Override
    public String toString() {
        return "MemberChangeEntry{" +
                "index=" + getIndex() +
                ", term=" + getTerm() +
                ", leaderId='" + getLeaderId() + '\'' +
                ", changeType=" + changeType +
                ", nodeId='" + nodeId + '\'' +
                ", address='" + address + '\'' +
                '}';
    }

    public static MemberChangeEntry add(long index, long term, String leaderId, String nodeId, String address) {
        return new MemberChangeEntry(index, term, leaderId, ChangeType.ADD, nodeId, address);
    }

    public static MemberChangeEntry remove(long index, long term, String leaderId, String nodeId) {
        return new MemberChangeEntry(index, term, leaderId, ChangeType.REMOVE, nodeId, null);
    }
}