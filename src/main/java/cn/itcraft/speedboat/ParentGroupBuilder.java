package cn.itcraft.speedboat;

import cn.itcraft.speedboat.config.SpeedboatConfigProvider;
import cn.itcraft.speedboat.persistence.PriorityStore;
import cn.itcraft.speedboat.persistence.RaftStore;
import cn.itcraft.speedboat.raft.ElectionTimeout;
import cn.itcraft.speedboat.raft.RaftNode;
import cn.itcraft.speedboat.serialize.CustomSerializer;
import cn.itcraft.speedboat.serialize.ProtostuffSerializer;
import cn.itcraft.speedboat.statemachine.NoopStateMachine;
import cn.itcraft.speedboat.strategy.consistency.ConsistencyPolicy;
import cn.itcraft.speedboat.strategy.voteweight.DatacenterPriorityTable;
import cn.itcraft.speedboat.strategy.voteweight.DatacenterPriorityVoteWeightStrategy;
import cn.itcraft.speedboat.strategy.voteweight.VoteWeightStrategy;
import cn.itcraft.speedboat.transport.NettyTransport;
import cn.itcraft.speedboat.transport.NodeEndpoint;
import cn.itcraft.speedboat.util.NetworkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 父组（跨机房层）组装器（非公开 API）。
 *
 * <p>职责（从 {@code Speedboat} 拆出，见 review 20261010-001 M1）：依配置与宿主身份
 * 构建父组的整套运行期组件——transport、RaftNode、优先级表、独立持久化与 peer 机房
 * 归属登记——并产出一个 {@link ParentGroupBundle}。宿主只持有束，不再散布 9 个字段。</p>
 *
 * <h3>并发契约</h3>
 * <p>仅在宿主 start 线程调用（构造期一次性），无并发问题。组件只构建不启动；
 * 启动顺序由宿主编排——子组的报告监听器要引用父组节点驱动"代表席位"门控，
 * 故父组必须先建、两组都建完后再统一 start。</p>
 *
 * @author speedboat
 * @since 1.2.0
 */
final class ParentGroupBuilder {

    private static final Logger logger = LoggerFactory.getLogger(ParentGroupBuilder.class);

    /** store 登记回调：宿主的 RaftStore 解析与停机收尾登记由此单向委托 */
    interface StoreSink {
        RaftStore build(String storeOwnerId);
    }

    private final SpeedboatConfigProvider config;
    private final String localIp;
    private final int localPort;
    private final String datacenterId;
    private final int datacenterIndex;
    private final ConsistencyPolicy consistencyPolicy;
    /** 领袖优先级模式（peer|dominant；三机房"同城双备+异地灾备"须 dominant 并指定主机房） */
    private final String leadershipMode;
    private final StoreSink storeSink;

    ParentGroupBuilder(SpeedboatConfigProvider config, String localIp, int localPort,
                       String datacenterId, int datacenterIndex, ConsistencyPolicy consistencyPolicy,
                       String leadershipMode, StoreSink storeSink) {
        this.config = config;
        this.localIp = localIp;
        this.localPort = localPort;
        this.datacenterId = datacenterId;
        this.datacenterIndex = datacenterIndex;
        this.consistencyPolicy = consistencyPolicy;
        this.leadershipMode = leadershipMode;
        this.storeSink = storeSink;
    }

    /**
     * 构建父组（跨机房层）。
     *
     * <p>父组是与子组<b>完全独立</b>的 Raft 实例：独立端口（子组端口 + {@code cross.port.offset}）、
     * 独立 term、独立日志与独立持久化目录。其投票成员是<b>各机房的子组 Leader</b>（"代表"），
     * 由代表席位门控保证同一机房同时只有一个代表参与。</p>
     *
     * <p><b>peer 取对侧机房全部节点</b>而非仅对侧 Leader：广播后只有对侧的子组 Leader
     * 会以活动态应答（非代表节点会拒绝投票），从而天然解决"对侧代表是谁"的发现问题，
     * 无需任何额外的服务发现。</p>
     *
     * @param allNodes 全部机房的节点地址列表
     * @return 父组组装产物束（未 start；宿主负责编排启动顺序）
     */
    ParentGroupBundle build(List<List<String>> allNodes) {
        final int offset = config.getCrossPortOffset();
        final int parentPort = localPort + offset;
        final String parentNodeId = NetworkUtils.generateNodeId(localIp + ":" + parentPort);

        // 全部机房标识（按 nodes.<索引> 顺序）：权重派生与 peer 机房归属都依赖它
        List<String> allDatacenterIds = new ArrayList<>();
        for (int i = 0; i < allNodes.size(); i++) {
            allDatacenterIds.add(Speedboat.resolveDatacenterId(config.getDatacenter(), true, i));
        }

        List<NodeEndpoint> parentPeerEndpoints = new ArrayList<>();
        Map<String, String> parentPeerDatacenters = new HashMap<>();
        List<String> parentPeerIds = new ArrayList<>();

        // 父组采用**全网互联**：peer 铺满所有机房的全部进程（仅本进程自身除外），
        // 而不是只连对侧机房。
        //
        // 为什么不能跳过本机房：父组成员是各机房的"子组 Leader"，而对侧代表运行期才确定，
        // 所以必须铺满对侧全部节点；但若因此把本机房的非代表进程也一并排除，它们就收不到
        // 任何父组心跳（本机房内没有别的进程会向它发），后果有二：
        //   1) parentLeader / parentTerm 在全网永不一致——非代表进程恒为 none / 0，
        //      监控无法用"全网 parentLeader 是否一致"来判断父组已收敛；
        //   2) 本机房子组换主后，新代表的父组 term 停在 0，只能靠逐轮选举一格格爬升才能
        //      追上当前任期，期间产生多轮无谓抖动，席位交接不"平滑"。
        //
        // 连上本机房的非代表进程是安全的，三重闸门已封死它参与选举的路径：
        //   1) 席位门控：不竞选（runElectionTimeout 已挡）、不投票、不授预票；
        //   2) 分母聚合：QuorumCalculator 按机房去重，同机房 peer 的权重恒不计入，
        //      故分母仍是 Σ机房权重，唯一性的数学推导完全不受影响；
        //   3) 它们只接收复制，事实上等同 Raft 的 learner（只跟随、不投票）。
        for (int dcIdx = 0; dcIdx < allNodes.size(); dcIdx++) {
            for (String nodeAddr : allNodes.get(dcIdx)) {
                String peerIp = NetworkUtils.parseIp(nodeAddr);
                int peerParentPort = NetworkUtils.parsePort(nodeAddr) + offset;
                String peerParentId = NetworkUtils.generateNodeId(peerIp + ":" + peerParentPort);

                // 剔除自身（双保险：按配置条目比对 + 按推导出的父组 nodeId 比对）
                if ((NetworkUtils.ipMatchesAddress(localIp, nodeAddr)
                        && NetworkUtils.parsePort(nodeAddr) == localPort)
                    || peerParentId.equals(parentNodeId)) {
                    continue;
                }

                parentPeerEndpoints.add(new NodeEndpoint(peerParentId, peerIp, peerParentPort));
                parentPeerIds.add(peerParentId);
                parentPeerDatacenters.put(peerParentId, allDatacenterIds.get(dcIdx));
            }
        }

        if (parentPeerIds.isEmpty()) {
            throw new IllegalStateException(
                "cross-datacenter mode requires at least one other parent process, but none were found "
                    + "(datacenterIndex=" + datacenterIndex + ", offset=" + offset + ")");
        }

        // 机房权重：索引派生（权重 = 机房总数 - 索引，索引靠前权重高）为基底，
        // 显式配置 datacenter.weight.<机房ID> 覆盖对应项，两者可部分混用。
        Map<String, Integer> effectiveWeights =
            new LinkedHashMap<String, Integer>(
                DatacenterPriorityVoteWeightStrategy.deriveWeightsByIndex(allDatacenterIds));
        effectiveWeights.putAll(config.getDatacenterWeights());

        // 阶段五：父组优先级表（可运行时换表）与投票策略共享同一实例。
        // 启动时若独立持久化文件存在，则以其为初值覆盖配置派生表（重启不丢提升状态）。
        DatacenterPriorityTable priorityTable =
            new DatacenterPriorityTable(
                effectiveWeights,
                DatacenterPriorityTable.Source.CONFIG);
        PriorityStore priorityStore =
            new PriorityStore(new File(config.getRaftPersistenceDir(), parentNodeId));
        DatacenterPriorityTable.Snapshot persistedPriority = priorityStore.load();
        if (persistedPriority != null && !persistedPriority.weights().isEmpty()) {
            priorityTable.replace(persistedPriority.epoch(), persistedPriority.weights(),
                persistedPriority.source());
            // 与恢复语义逐字一致：生效表以持久化快照整体替换（非混用）
            effectiveWeights.clear();
            effectiveWeights.putAll(persistedPriority.weights());
            logger.info("Parent priority restored from store: epoch={} source={} weights={}",
                persistedPriority.epoch(), persistedPriority.source(), persistedPriority.weights());
        }

        // 兜底权重 1：未登记的机房按最低权重，不会凭空获得多数派优势。
        // 四参构造把优先级表注入策略：策略每次权重计算读其最新快照。
        VoteWeightStrategy parentStrategy =
            new DatacenterPriorityVoteWeightStrategy(effectiveWeights, datacenterId, 1, priorityTable);
        // 领袖优先级策略绑定（此刻权重才真实可达）：dominant 注入父组投票策略，
        // sticky 让位与心跳冻结例外都随权重表热更新（人工提升/回退）自动变化
        cn.itcraft.speedboat.strategy.leadership.LeadershipPolicy leadershipPolicy =
            cn.itcraft.speedboat.strategy.leadership.LeadershipPolicy.MODE_DOMINANT.equals(leadershipMode)
                ? cn.itcraft.speedboat.strategy.leadership.LeadershipPolicy.dominant(parentStrategy)
                : cn.itcraft.speedboat.strategy.leadership.LeadershipPolicy.defaultPolicy();

        NodeEndpoint parentLocalEndpoint = new NodeEndpoint(parentNodeId, localIp, parentPort);
        NettyTransport parentNettyTransport = new NettyTransport(
            parentLocalEndpoint, parentPeerEndpoints, new CustomSerializer(new ProtostuffSerializer()));

        ElectionTimeout crossTimeout = new ElectionTimeout(
            config.getCrossDatacenterElectionTimeoutMin(),
            config.getCrossDatacenterElectionTimeoutMax());

        RaftNode parentRaftNode = new RaftNode.Builder()
            .nodeId(parentNodeId)
            .peerIds(parentPeerIds)
            .electionTimeout(crossTimeout)
            .transportLayer(parentNettyTransport)
            // 父组不复制业务状态（无锁表、无用户数据），但仍需状态机实例：
            // Leader 心跳折叠为空 AppendEntries 照常走 apply 路径，为 null 会 NPE
            .stateMachine(new NoopStateMachine())
            .datacenter(datacenterId)
            .voteWeightStrategy(parentStrategy)
            // 阶段五：优先级表 + 父侧独立持久化（人工升级 API 与重启恢复用）
            .priorityTable(priorityTable)
            .priorityStore(priorityStore)
            // 一致性策略（CP/AP）：仅父组消费。子组与单机房路径不注入即缺省 CP，
            // 机房内部选举不受降级接管影响——否则本机房内部分区会产出多个代表参与父组。
            // 注入构造期解析的同一实例，保证与门面人工切主闸门口径一致。
            .consistencyPolicy(consistencyPolicy)
            // 领袖优先级策略（三机房拓扑语义）：仅父组消费。dominant 模式下父组
            // PreVote sticky 为权重严格更高的候选者让位（主机房单向夺回通道）。
            .leadershipPolicy(leadershipPolicy)
            .maxLogSize(config.getMaxLogSize())
            .checkpointInterval(config.getRaftCheckpointInterval())
            // 独立 store：父组 term/votedFor 与子组互不污染（按父组 nodeId 分目录）
            .raftStore(storeSink.build(parentNodeId))
            .build();

        // peer 机房归属：权重策略据此计算对侧机房权重。
        // 未设置的 peer 会取本机房兜底，导致误判为同机房——故必须逐个登记。
        for (Map.Entry<String, String> entry : parentPeerDatacenters.entrySet()) {
            parentRaftNode.setPeerDatacenter(entry.getKey(), entry.getValue());
        }

        logger.info("Parent group built: nodeId={}, port={}, peers={}, datacenter={}, "
                + "weights={}, crossTimeout=[{}..{}]ms",
            parentNodeId, parentPort, parentPeerIds, datacenterId,
            effectiveWeights, crossTimeout.getMinMs(), crossTimeout.getMaxMs());

        return new ParentGroupBundle(parentRaftNode, parentNettyTransport, parentNodeId, parentPort,
            parentPeerEndpoints, parentPeerDatacenters, priorityTable, priorityStore, effectiveWeights);
    }
}
