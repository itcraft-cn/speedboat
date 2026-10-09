package cn.itcraft.speedboat;

import cn.itcraft.speedboat.config.SpeedboatConfigProvider;
import cn.itcraft.speedboat.raft.ElectionTimeout;
import cn.itcraft.speedboat.raft.NodeState;
import cn.itcraft.speedboat.raft.RaftGroup;
import cn.itcraft.speedboat.raft.RaftNode;
import cn.itcraft.speedboat.raft.report.RaftNodeReport;
import cn.itcraft.speedboat.persistence.MmapRaftStore;
import cn.itcraft.speedboat.persistence.InMemoryRaftStore;
import cn.itcraft.speedboat.persistence.NopRaftStore;
import cn.itcraft.speedboat.persistence.RaftStore;
import cn.itcraft.speedboat.serialize.CustomSerializer;
import cn.itcraft.speedboat.serialize.ProtostuffSerializer;
import cn.itcraft.speedboat.statemachine.NoopStateMachine;
import cn.itcraft.speedboat.strategy.voteweight.DatacenterPriorityVoteWeightStrategy;
import cn.itcraft.speedboat.strategy.voteweight.VoteWeightStrategy;
import cn.itcraft.speedboat.transport.NettyTransport;
import cn.itcraft.speedboat.transport.NodeEndpoint;
import cn.itcraft.speedboat.util.NetworkUtils;
import cn.itcraft.speedboat.lock.DistributedLock;
import cn.itcraft.speedboat.lock.DistributedLockImpl;
import cn.itcraft.speedboat.lock.LockStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Speedboat 门面类（极简 API）
 * 
 * <p>核心特性：</p>
 * <ul>
 *   <li>单例模式：Speedboat.start(config) 一行启动</li>
 *   <li>集群视角配置：用户只需关心 datacenter + nodes</li>
 *   <li>自动匹配：系统自动检测 hostname/IP 并匹配本节点</li>
 *   <li>自动模式：单机房扁平模式，跨机房级联模式</li>
 *   <li>内部概念隐藏：用户无需理解 RaftNode/NettyTransport 等内部类</li>
 * </ul>
 *
 * <p>使用示例：</p>
 * <pre>{@code
 * // 1. 创建配置
 * SpeedboatConfigProvider config = new PropertiesConfigProvider("config.properties");
 * 
 * // 2. 一行启动（单例模式）
 * Speedboat.start(config);
 * 
 * // 3. 查询状态
 * if (Speedboat.isMain()) {
 *     // 主节点逻辑
 * }
 * 
 * // 4. 停止
 * Speedboat.stop();
 * }</pre>
 *
 * <p>Properties 配置文件格式：</p>
 * <pre>
 * # 单机房场景（扁平模式）
 * nodes.0.0=192.168.10.1:3000
 * nodes.0.1=192.168.10.2:3000
 * nodes.0.2=192.168.10.3:3000
 * 
 * # 跨机房场景（级联模式）
 * datacenter=hangzhou001
 * nodes.0.0=192.168.10.1:3000
 * nodes.0.1=192.168.10.2:3000
 * nodes.0.2=192.168.10.3:3000
 * nodes.1.0=10.3.1.1:3000
 * nodes.1.1=10.3.1.2:3000
 * nodes.1.2=10.3.1.3:3000
 * 
 * # 可选：机房内选举超时（默认 1000-2000ms）
 * election.intra.timeout.min=1000
 * election.intra.timeout.max=2000
 * 
 * # 可选：机房间选举超时（默认 3000-5000ms）
 * election.cross.timeout.min=3000
 * election.cross.timeout.max=5000
 * </pre>
 *
 * @author speedboat
 * @since 1.0.0
 */
public class Speedboat {
    
    private static final Logger logger = LoggerFactory.getLogger(Speedboat.class);
    
    private static volatile Speedboat instance;
    
    private final SpeedboatConfigProvider config;
    private final String nodeId;
    private final String localIp;
    private final int localPort;
    private final String datacenterId;
    private final int datacenterIndex;
    private final boolean crossDatacenterMode;
    
    private NettyTransport nettyTransport;
    private RaftNode raftNode;
    private LockStateMachine lockStateMachine;
    private final ConcurrentHashMap<String, DistributedLock> lockCache = new ConcurrentHashMap<>();

    // ==================== 跨机房级联：父组（跨机房层）====================
    //
    // 每个进程持有两套彼此完全独立的 Raft 实例：
    //   子组（机房内层）——本机房 3 节点，选出"机房代表"
    //   父组（跨机房层）——成员 = 各机房的子组 Leader，选出"全局主"
    // 两组各有各的端口、term、日志与持久化目录，互不干扰。
    // 单机房模式下以下字段恒为 null。

    /** 父组传输层（绑定 localPort + cross.port.offset） */
    private NettyTransport parentNettyTransport;
    /** 父组 RaftNode */
    private RaftNode parentRaftNode;
    /** 父组节点标识（按父组端口推导，与子组 nodeId 区分） */
    private String parentNodeId;
    /** 父组绑定端口 */
    private int parentPort;
    
    private volatile boolean running;
    
    private Speedboat(SpeedboatConfigProvider config) {
        Objects.requireNonNull(config, "config cannot be null");
        this.config = config;
        
        List<List<String>> allNodes = config.getNodes();
        this.crossDatacenterMode = allNodes.size() > 1;
        
        this.localIp = NetworkUtils.detectLocalIp();
        
        NodeMatchResult matchResult = matchLocalNode(allNodes, localIp);
        this.localPort = matchResult.port;
        this.datacenterIndex = matchResult.datacenterIndex;
        this.datacenterId = resolveDatacenterId(config, datacenterIndex);
        
        // 身份确定性：自节点与 peer 节点均由 ip:port 推导，同一节点在所有进程内获得同一身份
        this.nodeId = NetworkUtils.generateNodeId(localIp + ":" + localPort);
        
        this.lockStateMachine = new LockStateMachine();
        
        logger.info("Speedboat initialized: nodeId={}, ip={}, port={}, datacenter={}, mode={}", 
            nodeId, localIp, localPort, datacenterId, 
            crossDatacenterMode ? "cross-datacenter(cascade)" : "single-datacenter(flat)");
        
        this.running = false;
    }
    
    private void doStart() {
        if (running) {
            logger.warn("Speedboat already running");
            return;
        }
        
        if (crossDatacenterMode) {
            doStartCrossDatacenterMode();
        } else {
            doStartSingleDatacenterMode();
        }
        
        running = true;
        logger.info("Speedboat started successfully");
    }
    
    private void doStartSingleDatacenterMode() {
        logger.info("Starting in single-datacenter (flat) mode...");
        
        List<List<String>> allNodes = config.getNodes();
        List<String> peerAddresses = collectPeerAddresses(allNodes, localIp);
        
        List<NodeEndpoint> peerEndpoints = new ArrayList<>();
        List<String> peerIds = new ArrayList<>();
        for (String peerAddr : peerAddresses) {
            String peerIp = NetworkUtils.parseIp(peerAddr);
            int peerPort = NetworkUtils.parsePort(peerAddr);
            // 身份确定性：peer 与自身使用同一生成规则（ip:port 推导），跨进程身份一致
            String peerNodeId = NetworkUtils.generateNodeId(peerAddr);
            
            peerEndpoints.add(new NodeEndpoint(peerNodeId, peerIp, peerPort));
            peerIds.add(peerNodeId);
        }
        
        NodeEndpoint localEndpoint = new NodeEndpoint(nodeId, localIp, localPort);
        CustomSerializer serializer = new CustomSerializer(new ProtostuffSerializer());
        nettyTransport = new NettyTransport(localEndpoint, peerEndpoints, serializer);

        // 持久化三档解析（P4：默认 mem；none=nop；mmap=可挂回）
        RaftStore resolvedStore = buildRaftStore(config);

        ElectionTimeout electionTimeout = new ElectionTimeout(
            config.getIntraDatacenterElectionTimeoutMin(),
            config.getIntraDatacenterElectionTimeoutMax()
        );

        raftNode = new RaftNode.Builder()
            .nodeId(nodeId)
            .peerIds(peerIds)
            .electionTimeout(electionTimeout)
            .transportLayer(nettyTransport)
            .stateMachine(lockStateMachine)
            .voteWeightStrategy(config.getVoteWeightStrategy())
            .maxLogSize(config.getMaxLogSize())
            .checkpointInterval(config.getRaftCheckpointInterval())
            .raftStore(resolvedStore)
            .build();

        logger.info("Starting NettyTransport...");
        nettyTransport.start();

        logger.info("Starting RaftNode...");
        raftNode.start();
    }
    
    private void doStartCrossDatacenterMode() {
        logger.info("Starting in cross-datacenter (cascade) mode...");

        List<List<String>> allNodes = config.getNodes();

        // 父组必须先建：子组的报告监听器要引用父组节点来驱动"代表席位"门控
        buildParentGroup(allNodes);

        // ==================== 子组（机房内层）====================
        List<String> localDcNodes = allNodes.get(datacenterIndex);
        List<String> localDcPeerAddresses = new ArrayList<>();

        for (String nodeAddr : localDcNodes) {
            if (!NetworkUtils.ipMatchesAddress(localIp, nodeAddr)) {
                localDcPeerAddresses.add(nodeAddr);
            }
        }

        List<NodeEndpoint> peerEndpoints = new ArrayList<>();
        List<String> peerIds = new ArrayList<>();
        for (String peerAddr : localDcPeerAddresses) {
            String peerIp = NetworkUtils.parseIp(peerAddr);
            int peerPort = NetworkUtils.parsePort(peerAddr);
            // 身份确定性：peer 与自身使用同一生成规则（ip:port 推导），跨进程身份一致
            String peerNodeId = NetworkUtils.generateNodeId(peerAddr);

            peerEndpoints.add(new NodeEndpoint(peerNodeId, peerIp, peerPort));
            peerIds.add(peerNodeId);
        }

        NodeEndpoint localEndpoint = new NodeEndpoint(nodeId, localIp, localPort);
        CustomSerializer serializer = new CustomSerializer(new ProtostuffSerializer());
        nettyTransport = new NettyTransport(localEndpoint, peerEndpoints, serializer);

        ElectionTimeout intraTimeout = new ElectionTimeout(
            config.getIntraDatacenterElectionTimeoutMin(),
            config.getIntraDatacenterElectionTimeoutMax()
        );

        raftNode = new RaftNode.Builder()
            .nodeId(nodeId)
            .peerIds(peerIds)
            .electionTimeout(intraTimeout)
            .transportLayer(nettyTransport)
            // defect-20260918-01：日志上限可配（重启副本判定状态的唯一重建依据）
            .maxLogSize(config.getMaxLogSize())
            .checkpointInterval(config.getRaftCheckpointInterval())
            .stateMachine(lockStateMachine)
            // 跨机房模式此前漏传投票权重策略，导致 DatacenterVoteWeightStrategy /
            // PreferNodeVoteWeightStrategy 在跨机房场景下完全不生效（等价于全员权重 1）
            .voteWeightStrategy(config.getVoteWeightStrategy())
            // 子组角色变化 → 驱动父组代表席位门控（ROLE_CHANGE 事件驱动，非轮询）
            .reportListener(this::onIntraRoleChange)
            .raftStore(buildRaftStore(config))
            .build();

        logger.info("Starting NettyTransport (intra-datacenter) on port {}...", localPort);
        nettyTransport.start();

        logger.info("Starting RaftNode (intra-datacenter group)...");
        raftNode.start();

        // ==================== 父组（跨机房层）====================
        // 初始不持有席位：只有本进程当选子组 Leader 后才代表本机房参与父组选举，
        // 否则同一机房会有多份"代表"同时投票，父组成员数随子组切换漂移。
        parentRaftNode.setSeatHeld(false);

        logger.info("Starting NettyTransport (inter-datacenter) on port {}...", parentPort);
        parentNettyTransport.start();

        logger.info("Starting RaftNode (inter-datacenter group)...");
        parentRaftNode.start();

        logger.info("Cross-datacenter cascade initialized: intra(nodeId={}, port={}), "
                + "inter(nodeId={}, port={}), weights={}",
            nodeId, localPort, parentNodeId, parentPort, parentDatacenterWeights);
    }

    /** 父组生效的机房权重表（日志与运维查询用） */
    private Map<String, Integer> parentDatacenterWeights = java.util.Collections.emptyMap();

    /**
     * 子组角色变化 → 驱动父组"代表席位"门控。
     *
     * <p>父组的投票成员是各机房的<b>子组 Leader</b>。同一机房同时只能有一个代表，
     * 否则父组票数会被虚增、多数派判定失去意义。</p>
     *
     * <p>本回调在 raft 单线程上触发（{@code ReportReason.ROLE_CHANGE}），
     * 因此是<b>事件驱动</b>而非轮询：子组切换到父组席位交接的时延接近 0，
     * 不受周期报告间隔（10s）影响。</p>
     *
     * @param report 子组状态快照
     */
    private void onIntraRoleChange(RaftNodeReport report) {
        if (parentRaftNode == null) {
            return;
        }
        parentRaftNode.setSeatHeld(report.getRole() == NodeState.LEADER);
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
     */
    private void buildParentGroup(List<List<String>> allNodes) {
        final int offset = config.getCrossPortOffset();
        this.parentPort = localPort + offset;
        this.parentNodeId = NetworkUtils.generateNodeId(localIp + ":" + parentPort);

        // 全部机房标识（按 nodes.<索引> 顺序）：权重派生与 peer 机房归属都依赖它
        List<String> allDatacenterIds = new ArrayList<>();
        for (int i = 0; i < allNodes.size(); i++) {
            allDatacenterIds.add(resolveDatacenterId(config, i));
        }

        List<NodeEndpoint> parentPeerEndpoints = new ArrayList<>();
        List<String> parentPeerIds = new ArrayList<>();
        Map<String, String> parentPeerDatacenters = new HashMap<>();

        for (int dcIdx = 0; dcIdx < allNodes.size(); dcIdx++) {
            if (dcIdx == datacenterIndex) {
                // 跳过本机房：父组成员是"本机房的代表"，不是本机房的所有节点
                continue;
            }
            for (String nodeAddr : allNodes.get(dcIdx)) {
                String peerIp = NetworkUtils.parseIp(nodeAddr);
                int peerParentPort = NetworkUtils.parsePort(nodeAddr) + offset;
                String peerParentId = NetworkUtils.generateNodeId(peerIp + ":" + peerParentPort);

                parentPeerEndpoints.add(new NodeEndpoint(peerParentId, peerIp, peerParentPort));
                parentPeerIds.add(peerParentId);
                parentPeerDatacenters.put(peerParentId, allDatacenterIds.get(dcIdx));
            }
        }

        if (parentPeerIds.isEmpty()) {
            throw new IllegalStateException(
                "cross-datacenter mode requires peers in other datacenters, but none were found "
                    + "(datacenterIndex=" + datacenterIndex + ", offset=" + offset + ")");
        }

        // 机房权重：索引派生（权重 = 机房总数 - 索引，索引靠前权重高）为基底，
        // 显式配置 datacenter.weight.<机房ID> 覆盖对应项，两者可部分混用。
        Map<String, Integer> effectiveWeights =
            new LinkedHashMap<String, Integer>(
                DatacenterPriorityVoteWeightStrategy.deriveWeightsByIndex(allDatacenterIds));
        effectiveWeights.putAll(config.getDatacenterWeights());
        this.parentDatacenterWeights = effectiveWeights;

        // 兜底权重 1：未登记的机房按最低权重，不会凭空获得多数派优势
        VoteWeightStrategy parentStrategy =
            new DatacenterPriorityVoteWeightStrategy(effectiveWeights, datacenterId, 1);

        NodeEndpoint parentLocalEndpoint = new NodeEndpoint(parentNodeId, localIp, parentPort);
        parentNettyTransport = new NettyTransport(
            parentLocalEndpoint, parentPeerEndpoints, new CustomSerializer(new ProtostuffSerializer()));

        ElectionTimeout crossTimeout = new ElectionTimeout(
            config.getCrossDatacenterElectionTimeoutMin(),
            config.getCrossDatacenterElectionTimeoutMax());

        parentRaftNode = new RaftNode.Builder()
            .nodeId(parentNodeId)
            .peerIds(parentPeerIds)
            .electionTimeout(crossTimeout)
            .transportLayer(parentNettyTransport)
            // 父组不复制业务状态（无锁表、无用户数据），但仍需状态机实例：
            // Leader 心跳折叠为空 AppendEntries 照常走 apply 路径，为 null 会 NPE
            .stateMachine(new NoopStateMachine())
            .datacenter(datacenterId)
            .voteWeightStrategy(parentStrategy)
            .maxLogSize(config.getMaxLogSize())
            .checkpointInterval(config.getRaftCheckpointInterval())
            // 独立 store：父组 term/votedFor 与子组互不污染（按父组 nodeId 分目录）
            .raftStore(buildRaftStore(config, parentNodeId))
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
    }
    
    private void doStop() {
        if (!running) {
            logger.warn("Speedboat not running");
            return;
        }
        
        logger.info("Stopping Speedboat...");
        
        for (DistributedLock lock : lockCache.values()) {
            if (lock instanceof DistributedLockImpl) {
                ((DistributedLockImpl) lock).shutdown();
            }
        }
        lockCache.clear();
        
        // 父组先停：父组的"全局主"语义依赖子组席位，先拆上层再拆下层
        if (parentRaftNode != null) {
            parentRaftNode.shutdown();
            parentRaftNode = null;
        }
        if (parentNettyTransport != null) {
            parentNettyTransport.shutdown();
            parentNettyTransport = null;
        }

        if (raftNode != null) {
            raftNode.shutdown();
        }

        if (nettyTransport != null) {
            nettyTransport.shutdown();
        }

        // 持久化收尾（Mmap 档：msync 尾部 + 关闭映射；mem 档无操作）。
        // 子组与父组各有一份 store，需全部关闭，否则父组的 mmap 映射会泄漏。
        for (RaftStore store : raftStoreRefs) {
            try {
                if (store instanceof MmapRaftStore) {
                    ((MmapRaftStore) store).close();
                }
            } catch (Exception e) {
                logger.warn("RaftStore close failed: {}", e.toString());
            }
        }
        raftStoreRefs.clear();

        running = false;
        logger.info("Speedboat stopped");
    }

    /** 本进程创建的全部 RaftStore（子组 + 父组），停机时统一收尾关闭 */
    private final List<RaftStore> raftStoreRefs = new ArrayList<>();

    /**
     * 持久化三档解析（2026-09-18 P4-M5 收敛：**生产缺省 mmap**）。
     *
     * <p>决策依据：写路径 Mem/Mmap 同数量级（低频锁命令下均无感），但
     * <b>跨进程重启安全性 Mmap 严格占优</b>——Mem 档进程重启即丢 term/votedFor
     * （重复投票风险）与锁 epoch；故生产门面缺省应携带最全安全性档位。</p>
     * <ul>
     *   <li>{@code none} → NopRaftStore（测试基线/显式关闭）</li>
     *   <li>{@code mem} → InMemoryRaftStore（<b>显式档</b>：无盘沙箱/容器 ephemeral；跨进程重启丢 term 与锁状态）</li>
     *   <li>{@code mmap}（缺省）→ MmapRaftStore（跨进程重启挂回；目录 {nodeId} 自动分目录）</li>
     * </ul>
     * 库级 {@code RaftNode.Builder} 缺省保持 Nop（零副作用，档位由调用方显式选择）。
     */
    private RaftStore buildRaftStore(SpeedboatConfigProvider config) {
        return buildRaftStore(config, nodeId);
    }

    /**
     * 持久化三档解析（指定归属节点）。
     *
     * <p>跨机房级联模式下子组与父组<b>必须各有一份独立 store</b>：两组的 term/votedFor
     * 互相独立，共用会让父组的任期污染子组（或反之），重启恢复时无法分辨属于哪一组。
     * 故按 {@code storeOwnerId}（子组/父组各自的 nodeId）分目录。</p>
     *
     * @param config        配置提供者
     * @param storeOwnerId  store 归属节点标识，用于分目录与文件名占位
     * @return 解析出的 RaftStore，并登记到 {@link #raftStoreRefs} 供停机收尾
     */
    private RaftStore buildRaftStore(SpeedboatConfigProvider config, String storeOwnerId) {
        String type = config.getRaftPersistenceType() == null ? "mmap" : config.getRaftPersistenceType().trim();
        switch (type) {
            case "none":
                return NopRaftStore.getInstance();
            case "mem": {
                RaftStore store = InMemoryRaftStore.createDefault();
                raftStoreRefs.add(store);
                return store;
            }
            case "mmap":
            default: {
                String dir = config.getRaftPersistenceDir();
                String fileName = config.getRaftMmapFileName().replace("{nodeId}", storeOwnerId);
                File dirFile = new File(dir, storeOwnerId);
                RaftStore store = new MmapRaftStore(dirFile, fileName, config.getRaftMmapSizeMb());
                raftStoreRefs.add(store);
                logger.info("RaftStore resolved mmap: ownerId={}, dir={}, file={}, sizeMb={}",
                    storeOwnerId, dirFile.getAbsolutePath(), fileName, config.getRaftMmapSizeMb());
                return store;
            }
        }
    }
    
    private boolean checkIsMain() {
        checkRunning();
        if (raftNode == null || !raftNode.isLeader()) {
            return false;
        }
        // 跨机房级联：全局主 = 本进程既是子组（机房内）Leader，
        // 又在父组（跨机房）中当选。两道条件缺一不可——
        // 仅子组当选只代表"本机房代表"，不等于"全局主"。
        // parentRaftNode 为 null 表示单机房模式，子组 Leader 即全局主。
        return parentRaftNode == null || parentRaftNode.isLeader();
    }
    
    private String doGetLeaderId() {
        checkRunning();
        return raftNode != null ? raftNode.getLeaderId() : null;
    }
    
    private long doGetTerm() {
        checkRunning();
        return raftNode != null ? raftNode.getTerm().getCurrent() : 0;
    }
    
    private DistributedLock doGetLock(String lockName) {
        checkRunning();
        Objects.requireNonNull(lockName, "lockName cannot be null");
        
        return lockCache.computeIfAbsent(lockName, name -> 
            new DistributedLockImpl(name, nodeId, raftNode, lockStateMachine, config.getLockLeaseMs())
        );
    }
    
    private void checkRunning() {
        if (!running) {
            throw new IllegalStateException("Speedboat not running");
        }
    }
    
    private NodeMatchResult matchLocalNode(List<List<String>> allNodes, String localIp) {
        for (int dcIndex = 0; dcIndex < allNodes.size(); dcIndex++) {
            List<String> dcNodes = allNodes.get(dcIndex);
            for (String nodeAddr : dcNodes) {
                if (NetworkUtils.ipMatchesAddress(localIp, nodeAddr)) {
                    int port = NetworkUtils.parsePort(nodeAddr);
                    logger.info("Matched local node: ip={}, port={}, datacenterIndex={}", 
                        localIp, port, dcIndex);
                    return new NodeMatchResult(dcIndex, port);
                }
            }
        }
        
        throw new IllegalArgumentException(
            "Local IP " + localIp + " not found in configured nodes");
    }
    
    /**
     * 解析本进程所属机房标识（实例入口，委托给静态纯函数以便单测）。
     *
     * @param config          配置提供者
     * @param datacenterIndex 本机房在 {@code nodes.<索引>.*} 中的索引
     * @return 本进程所属机房标识
     * @see #resolveDatacenterId(String, boolean, int)
     */
    private String resolveDatacenterId(SpeedboatConfigProvider config, int datacenterIndex) {
        return resolveDatacenterId(config.getDatacenter(), crossDatacenterMode, datacenterIndex);
    }

    /**
     * 解析机房标识（静态纯函数，无副作用，便于穷举测试）。
     *
     * <p><b>单机房模式</b>：{@code datacenter=} 配置原样生效（保持既有部署的向后兼容），
     * 未配置则回退为 {@code dc-<索引>}（单机房下恒为 {@code dc-0}）。</p>
     *
     * <p><b>跨机房模式</b>：机房标识<b>必须按索引唯一</b>。两个机房使用的是同一份配置文件，
     * 若直接返回 {@code datacenter=} 配置值，主机房与备机房会解析出<b>同一个</b> ID，
     * 导致一切按机房归属判权的策略（机房优先级权重、同城提权等）完全失效——
     * 每个节点都会认为对端与自己同机房。故跨机房模式下在配置值后追加 {@code -<索引>}。</p>
     *
     * @param configuredDatacenter {@code datacenter=} 配置值，可为 null
     * @param crossDatacenterMode  是否跨机房模式（{@code nodes} 存在多组）
     * @param datacenterIndex      本机房在 {@code nodes.<索引>.*} 中的索引
     * @return 本进程所属机房标识
     */
    static String resolveDatacenterId(String configuredDatacenter, boolean crossDatacenterMode, int datacenterIndex) {
        if (!crossDatacenterMode) {
            return configuredDatacenter != null ? configuredDatacenter : "dc-" + datacenterIndex;
        }

        String base = configuredDatacenter != null ? configuredDatacenter : "dc";
        return base + "-" + datacenterIndex;
    }
    
    private List<String> collectPeerAddresses(List<List<String>> allNodes, String localIp) {
        List<String> peerAddresses = new ArrayList<>();
        
        for (List<String> dcNodes : allNodes) {
            for (String nodeAddr : dcNodes) {
                if (!NetworkUtils.ipMatchesAddress(localIp, nodeAddr)) {
                    peerAddresses.add(nodeAddr);
                }
            }
        }
        
        logger.info("Collected {} peer addresses", peerAddresses.size());
        return peerAddresses;
    }
    
    private static class NodeMatchResult {
        final int datacenterIndex;
        final int port;
        
        NodeMatchResult(int datacenterIndex, int port) {
            this.datacenterIndex = datacenterIndex;
            this.port = port;
        }
    }
    
    public static synchronized void start(SpeedboatConfigProvider config) {
        if (instance != null && instance.running) {
            logger.warn("Speedboat singleton already running");
            return;
        }
        
        instance = new Speedboat(config);
        instance.doStart();
    }
    
    public static synchronized void stop() {
        if (instance == null) {
            logger.warn("Speedboat singleton not initialized");
            return;
        }
        
        instance.doStop();
        instance = null;
    }
    
    public static boolean isMain() {
        checkSingletonRunning();
        return instance.checkIsMain();
    }
    
    public static String getLeaderId() {
        checkSingletonRunning();
        return instance.doGetLeaderId();
    }
    
    public static long getTerm() {
        checkSingletonRunning();
        return instance.doGetTerm();
    }
    
    public static String getNodeId() {
        checkSingletonRunning();
        return instance.nodeId;
    }
    
    public static String getDatacenterId() {
        checkSingletonRunning();
        return instance.datacenterId;
    }
    
    public static boolean isRunning() {
        return instance != null && instance.running;
    }
    
    public static DistributedLock getLock(String lockName) {
        checkSingletonRunning();
        return instance.doGetLock(lockName);
    }
    
    private static void checkSingletonRunning() {
        if (instance == null || !instance.running) {
            throw new IllegalStateException("Speedboat singleton not running");
        }
    }
}