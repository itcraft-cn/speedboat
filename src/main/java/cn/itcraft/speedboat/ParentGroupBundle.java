package cn.itcraft.speedboat;

import cn.itcraft.speedboat.persistence.PriorityStore;
import cn.itcraft.speedboat.raft.RaftNode;
import cn.itcraft.speedboat.strategy.voteweight.DatacenterPriorityTable;
import cn.itcraft.speedboat.transport.NettyTransport;
import cn.itcraft.speedboat.transport.NodeEndpoint;

import java.util.List;
import java.util.Map;

/**
 * 父组（跨机房层）组装产物束（非公开 API）。
 *
 * <p>由 {@link ParentGroupBuilder} 一次性产出，宿主 {@code Speedboat} 以单一字段持有。
 * 把"父组的全部运行期组件 + 探测/闸门所需结构"收敛为一个不可变束，
 * 门面侧字段数量从 9 个降到 1 个（review 20261010-001 M1）。</p>
 *
 * @author speedboat
 * @since 1.2.0
 */
final class ParentGroupBundle {

    /** 父组 RaftNode */
    final RaftNode raftNode;
    /** 父组传输层（绑定 localPort + cross.port.offset） */
    final NettyTransport transport;
    /** 父组节点标识（按父组端口推导，与子组 nodeId 区分） */
    final String nodeId;
    /** 父组绑定端口 */
    final int port;
    /** 父组全部 peer 端点（含对侧与本机房非代表进程；阶段五连通性闸门探测源） */
    final List<NodeEndpoint> peerEndpoints;
    /** 父组各 peer 的机房标识（闸门据此只探"对侧机房"端点） */
    final Map<String, String> peerDatacenters;
    /** 父组优先级表（阶段五人工升级；与父组投票策略共享同一实例） */
    final DatacenterPriorityTable priorityTable;
    /** 父侧独立优先级持久化（阶段五；重启不丢提升状态） */
    final PriorityStore priorityStore;
    /** 生效的机房权重表（日志与运维查询用） */
    final Map<String, Integer> effectiveWeights;

    ParentGroupBundle(RaftNode raftNode, NettyTransport transport, String nodeId, int port,
                      List<NodeEndpoint> peerEndpoints, Map<String, String> peerDatacenters,
                      DatacenterPriorityTable priorityTable, PriorityStore priorityStore,
                      Map<String, Integer> effectiveWeights) {
        this.raftNode = raftNode;
        this.transport = transport;
        this.nodeId = nodeId;
        this.port = port;
        this.peerEndpoints = peerEndpoints;
        this.peerDatacenters = peerDatacenters;
        this.priorityTable = priorityTable;
        this.priorityStore = priorityStore;
        this.effectiveWeights = effectiveWeights;
    }
}
