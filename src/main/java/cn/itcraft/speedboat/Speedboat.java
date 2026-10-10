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
import cn.itcraft.speedboat.persistence.PriorityStore;
import cn.itcraft.speedboat.strategy.consistency.ConsistencyPolicy;
import cn.itcraft.speedboat.strategy.voteweight.DatacenterPriorityTable;
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
    /** 一致性策略（CP/AP）：启动时选定、全生命周期恒定。
     *
     * <p>门面与父组 RaftNode <b>共用同一实例</b>——人工切主闸门若另行构造策略，
     * 两处判定口径可能不一致（策略对象还携带降级态等运行期状态）。
     * 仅读取 {@code mode()}，不触碰降级状态，故跨线程读安全。</p>
     */
    private final ConsistencyPolicy consistencyPolicy;
    /** 人工切主网关（Phase 五运维兜底实例级编排体） */
    private final PromoteGateway promoteGateway;
    /** 父组组装产物束；单机房模式下恒为 null */
    private ParentGroupBundle parentGroup;
    
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

    
    private volatile boolean running;
    
    private Speedboat(SpeedboatConfigProvider config) {
        Objects.requireNonNull(config, "config cannot be null");
        this.config = config;
        
        List<List<String>> allNodes = config.getNodes();
        this.crossDatacenterMode = allNodes.size() > 1;
        // CP/AP 策略在构造期解析一次，供门面人工切主闸门与父组 RaftNode 共用；
        // 提供方未实现该方法（返回 null）时按缺省 CP 兜底，与 NodeContext 行为一致。
        ConsistencyPolicy resolvedPolicy =
            config.getConsistencyPolicy();
        this.consistencyPolicy = resolvedPolicy != null
            ? resolvedPolicy
            : cpFallbackPolicy();
        this.promoteGateway = new PromoteGateway(this);

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
        List<String> peerAddresses = collectPeerAddresses(allNodes, localIp, localPort);
        
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
        buildParentGroup();

        // ==================== 子组（机房内层）====================
        List<String> localDcNodes = allNodes.get(datacenterIndex);
        List<String> localDcPeerAddresses = new ArrayList<>();

        for (String nodeAddr : localDcNodes) {
            // 按 ip:port 精确排除自身（同 IP 多进程下仅按 IP 会误剔本机房其余进程）
            if (!isSelfEntry(localIp, localPort, nodeAddr)) {
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
        parentGroup.raftNode.setSeatHeld(false);

        logger.info("Starting NettyTransport (inter-datacenter) on port {}...", parentGroup.port);
        parentGroup.transport.start();

        logger.info("Starting RaftNode (inter-datacenter group)...");
        parentGroup.raftNode.start();

        logger.info("Cross-datacenter cascade initialized: intra(nodeId={}, port={}), "
                + "inter(nodeId={}, port={}), weights={}",
            nodeId, localPort, parentGroup.nodeId, parentGroup.port, parentGroup.effectiveWeights);
    }


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
        if (parentGroup == null) {
            return;
        }
        parentGroup.raftNode.setSeatHeld(report.getRole() == NodeState.LEADER);
    }

    
    /**
     * CP 兜底单例（提供方未实现 {@code getConsistencyPolicy} 时）；独立方法便于一处注释
     */
    private static ConsistencyPolicy cpFallbackPolicy() {
        return cn.itcraft.speedboat.strategy.consistency.CpConsistencyPolicy.getInstance();
    }

    // ==================== PromoteGateway 桥接访问器（包内可见） ====================
    //
    // 对齐 tiny-rules 的"Shadow 直读 State"形态：网关不深挖宿主内部结构，
    // 一律经本组包内访问器取状态——宿主字段布局后续变化时改动收敛于此一处。

    ConsistencyPolicy policy() {
        return consistencyPolicy;
    }

    SpeedboatConfigProvider config() {
        return config;
    }

    boolean isCrossDatacenterMode() {
        return crossDatacenterMode;
    }

    String dcId() {
        return datacenterId;
    }

    String nodeId() {
        return nodeId;
    }

    RaftNode parentRaftNodeOrNull() {
        return parentGroup == null ? null : parentGroup.raftNode;
    }

    List<NodeEndpoint> parentPeerEndpoints() {
        return parentGroup == null
            ? java.util.Collections.<NodeEndpoint>emptyList()
            : parentGroup.peerEndpoints;
    }

    Map<String, String> parentPeerDatacenters() {
        return parentGroup == null
            ? java.util.Collections.<String, String>emptyMap()
            : parentGroup.peerDatacenters;
    }

    /**
     * 构建父组（跨机房层），组装逻辑全部收敛至 {@link ParentGroupBuilder}。
     *
     * <p>宿主只负责三件事：提供身份参数、把 RaftStore 解析/停机登记以回调注入
     * （父组建在这套登记链上，与子组共用同一收尾）、以单一束字段持有产物。</p>
     */
    private void buildParentGroup() {
        ParentGroupBuilder builder = new ParentGroupBuilder(
            config, localIp, localPort, datacenterId, datacenterIndex, consistencyPolicy,
            ownerId -> buildRaftStore(config, ownerId));
        this.parentGroup = builder.build(config.getNodes());
    }

    private void doStop() {
        if (!running) {
            logger.warn("Speedboat not running");
            return;
        }

        logger.info("Stopping Speedboat...");

        // 拆卸序（review L4）：先摘 running 标志再拆组件——本方法若在并发观测/人工切主
        // API 读到组件后、其置 null 前的窄窗口，API 会以"not-cross-datacenter-mode"为因
        // 拒绝（cause 失真）。先摘标志让一切 API 立即以 singleton-not-running 的准确口径拒绝。
        running = false;

        for (DistributedLock lock : lockCache.values()) {
            if (lock instanceof DistributedLockImpl) {
                ((DistributedLockImpl) lock).shutdown();
            }
        }
        lockCache.clear();
        
        // 父组先停：父组的"全局主"语义依赖子组席位，先拆上层再拆下层
        if (parentGroup != null) {
            parentGroup.raftNode.shutdown();
            parentGroup.transport.shutdown();
            parentGroup = null;
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
        // parentGroup 为 null 表示单机房模式，子组 Leader 即全局主。
        return parentGroup == null || parentGroup.raftNode.isLeader();
    }
    
    private String doGetLeaderId() {
        checkRunning();
        return raftNode != null ? raftNode.getLeaderId() : null;
    }
    
    private long doGetTerm() {
        checkRunning();
        return raftNode != null ? raftNode.getTerm().getCurrent() : 0;
    }

    // ==================== 跨机房级联：双层观测 ====================

    /**
     * 本进程是否为<b>子组（机房内层）Leader</b>，即"本机房代表"。
     *
     * <p>拥有本标志只代表被本机房推举为发言人，<b>不等于</b>全局主——
     * 仍需在父组中当选才构成 {@link #isMain()}。备机房在主机房存活期间
     * 通常长期 {@code isIntraLeader()=true} 但 {@code isMain()=false}，属预期形态。</p>
     *
     * @return 是否子组 Leader；单机房模式下与 {@link #isMain()} 同义
     */
    private boolean doIsIntraLeader() {
        checkRunning();
        return raftNode != null && raftNode.isLeader();
    }

    /**
     * 本进程是否为<b>父组（跨机房层）Leader</b>。
     *
     * <p>父组成员是各机房的子组 Leader，因此非代表进程恒为 false。
     * 单机房模式无父组，恒为 true（此时子组 Leader 即全局主）。</p>
     *
     * @return 是否父组 Leader
     */
    private boolean doIsParentLeader() {
        checkRunning();
        return parentGroup == null || parentGroup.raftNode.isLeader();
    }

    /** 子组（机房内层）term —— 与父组 term 完全独立、各自单调。 */
    private long doGetIntraTerm() {
        return doGetTerm();
    }

    /** 父组（跨机房层）term；单机房模式无父组，返回 0。 */
    private long doGetParentTerm() {
        checkRunning();
        return parentGroup != null ? parentGroup.raftNode.getTerm().getCurrent() : 0L;
    }

    /** 子组当前已知 Leader（机房内视角）；单机房模式即全局 Leader。 */
    private String doGetIntraLeaderId() {
        return doGetLeaderId();
    }

    /** 父组当前已知 Leader 的父组 nodeId；单机房模式无父组，返回 null。 */
    private String doGetParentLeaderId() {
        checkRunning();
        return parentGroup != null ? parentGroup.raftNode.getLeaderId() : null;
    }

    /**
     * 父组（跨机房层）节点标识。
     *
     * <p>按父组端口推导（{@code ip:(子组端口 + cross.port.offset)}），与子组 nodeId
     * 同规则但不同值——两组是彼此独立的节点身份，必须可区分。</p>
     *
     * @return 父组 nodeId；单机房模式无父组，返回 null
     */
    private String doGetParentNodeId() {
        return parentGroup == null ? null : parentGroup.nodeId;
    }

    /** 父组绑定端口；单机房模式返回 0。 */
    private int doGetParentPort() {
        return parentGroup == null ? 0 : parentGroup.port;
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
        return resolveLocalEntry(allNodes, localIp, localPortOverride());
    }

    /**
     * 可选的本进程端口钉定（系统属性 {@code speedboat.local.port}）。
     *
     * <p>单机多进程部署（同 IP 多端口，如阶段六备机房三进程共享 172.22.133.1）
     * 下仅凭 IP 无法区分身份，必须显式钉定端口。未设置时返回 {@code null}，
     * 走历史"按 IP 取首条"路径，既有部署行为完全不变。</p>
     *
     * @return 端口整数；未设置或非法时为 null
     */
    static Integer localPortOverride() {
        String prop = System.getProperty("speedboat.local.port");
        if (prop == null || prop.trim().isEmpty()) {
            prop = System.getenv("SPEEDBOAT_LOCAL_PORT");
        }
        if (prop == null || prop.trim().isEmpty()) {
            return null;
        }
        try {
            return Integer.parseInt(prop.trim());
        } catch (NumberFormatException e) {
            logger.warn("Invalid speedboat.local.port value: {}, falling back to ip-only match", prop);
            return null;
        }
    }

    /**
     * 解析本进程在 nodes 配置中的条目（静态纯函数，便于穷举测试）。
     *
     * <p><b>端口钉定模式</b>（{@code localPort != null}）：要求 IP <b>与</b>端口同时精确匹配，
     * 否则抛错——这是同 IP 多进程（单机多进程部署）的唯一无歧义身份来源。</p>
     *
     * <p><b>历史模式</b>（{@code localPort == null}）：按 IP 匹配取首个条目，
     * 与引入本方法前的行为逐条等价（既有单机单进程部署不受影响）。</p>
     *
     * @param allNodes   全网节点（按机房分组）
     * @param localIp    本进程 IP（speedboat.local.ip 钉定值）
     * @param localPort  端口钉定值；null 走历史 IP-首条匹配
     * @return 命中的条目（机房索引 + 端口）
     */
    static NodeMatchResult resolveLocalEntry(List<List<String>> allNodes, String localIp, Integer localPort) {
        if (localPort != null) {
            for (int dcIndex = 0; dcIndex < allNodes.size(); dcIndex++) {
                for (String nodeAddr : allNodes.get(dcIndex)) {
                    if (NetworkUtils.ipMatchesAddress(localIp, nodeAddr)
                            && NetworkUtils.parsePort(nodeAddr) == localPort) {
                        logger.info("Matched local node by ip:port: ip={}, port={}, datacenterIndex={}",
                            localIp, localPort, dcIndex);
                        return new NodeMatchResult(dcIndex, localPort);
                    }
                }
            }
            throw new IllegalArgumentException(
                "Local entry " + localIp + ":" + localPort + " not found in configured nodes "
                    + "(speedboat.local.port is set but no matching ip:port entry exists)");
        }

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
     * 配置条目是否为本进程自身（IP <b>与</b>端口同时匹配才算）。
     *
     * <p>身份排除必须按 {@code ip:port} 精确比对：同 IP 多进程（单机多进程部署）下
     * 若仅按 IP 排除，本机房的其余同 IP 进程会被误当作"自己"而从 peers 中剔除——
     * 子组直接失去同房成员、单机房模式 peerIds 变空。既有单机单进程部署下
     * 每机 IP 唯一，精确比对与 IP 比对结果相同，行为不变。</p>
     *
     * @param localIp   本进程 IP
     * @param localPort 本进程端口（已由 matchLocalNode 钉定）
     * @param nodeAddr  待比对的配置条目（ip:port）
     * @return true 表示该条目就是本进程
     */
    private static boolean isSelfEntry(String localIp, int localPort, String nodeAddr) {
        return NetworkUtils.ipMatchesAddress(localIp, nodeAddr)
            && NetworkUtils.parsePort(nodeAddr) == localPort;
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
    
    private List<String> collectPeerAddresses(List<List<String>> allNodes, String localIp, int localPort) {
        List<String> peerAddresses = new ArrayList<>();
        
        for (List<String> dcNodes : allNodes) {
            for (String nodeAddr : dcNodes) {
                // 按 ip:port 精确排除自身：同 IP 多端口（单机多进程）下
                // 仅按 IP 会把同机其余进程一并剔除，peerIds 直接变空
                if (!isSelfEntry(localIp, localPort, nodeAddr)) {
                    peerAddresses.add(nodeAddr);
                }
            }
        }
        
        logger.info("Collected {} peer addresses", peerAddresses.size());
        return peerAddresses;
    }
    
    static class NodeMatchResult {
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

    /**
     * 本进程是否为子组（机房内层）Leader，即"本机房代表"。
     *
     * <p>跨机房级联模式下它<b>不等于</b> {@link #isMain()}：代表还需在父组当选才构成全局主。
     * 单机房模式下与 {@link #isMain()} 同义。</p>
     *
     * @return 是否子组 Leader
     */
    public static boolean isIntraLeader() {
        checkSingletonRunning();
        return instance.doIsIntraLeader();
    }

    /**
     * 本进程是否为父组（跨机房层）Leader。
     *
     * <p>{@code isMain() == isIntraLeader() && isParentLeader()}。</p>
     *
     * @return 是否父组 Leader；单机房模式恒为 true
     */
    public static boolean isParentLeader() {
        checkSingletonRunning();
        return instance.doIsParentLeader();
    }

    /**
     * 子组（机房内层）term，与父组 term 完全独立、各自单调。
     *
     * @return 子组当前任期
     */
    public static long getIntraTerm() {
        checkSingletonRunning();
        return instance.doGetIntraTerm();
    }

    /**
     * 父组（跨机房层）term，与子组 term 完全独立、各自单调。
     *
     * <p>用于验证两组任期互不污染：子组选举推进子组 term，父组选举推进父组 term，
     * 二者没有换算关系。单机房模式无父组，返回 0。</p>
     *
     * @return 父组当前任期
     */
    public static long getParentTerm() {
        checkSingletonRunning();
        return instance.doGetParentTerm();
    }

    /**
     * 子组当前已知 Leader 的 nodeId（机房内视角）。
     *
     * @return 子组 Leader nodeId；无则返回 null
     */
    public static String getIntraLeaderId() {
        checkSingletonRunning();
        return instance.doGetIntraLeaderId();
    }

    /**
     * 父组当前已知 Leader 的<b>父组</b> nodeId（跨机房视角）。
     *
     * <p>注意其取值域与 {@link #getIntraLeaderId()} 不同（按父组端口推导），两者不可混用比较。
     * 单机房模式无父组，返回 null。</p>
     *
     * @return 父组 Leader nodeId；无则返回 null
     */
    public static String getParentLeaderId() {
        checkSingletonRunning();
        return instance.doGetParentLeaderId();
    }

    /**
     * 父组（跨机房层）节点标识，按父组端口推导，与子组 {@link #getNodeId()} 可区分。
     *
     * @return 父组 nodeId；单机房模式无父组，返回 null
     */
    public static String getParentNodeId() {
        checkSingletonRunning();
        return instance.doGetParentNodeId();
    }

    /**
     * 父组绑定端口（{@code 子组端口 + cross.port.offset}）。
     *
     * <p>用于部署侧核对防火墙/端口清单；单机房模式返回 0。</p>
     *
     * @return 父组端口
     */
    public static int getParentPort() {
        checkSingletonRunning();
        return instance.doGetParentPort();
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

    // ==================== 人工升级 API（阶段五运维兜底） ====================

    /**
     * 人工提升本机房在父组的优先级（阶段五运维兜底）。
     *
     * <p>仅在跨机房模式下有效。本方法是<b>唯一入口</b>——不做远程接口、不做控制台。
     * 接受前依次校验：单例在跑、<b>CP 模式</b>、跨机房模式、操作者非空、目标机房等于本机房；
     * 随后执行<b>连通性闸门</b>：探对侧机房全部父组端点，<b>任一可达即拒绝</b>
     * （此时强行提升会打开"双主"窗口，唯一性保证被破坏）。全部不可达才放行到
     * raft 线程执行换表 + term 跃升 + 登基 + 复制 + 持久化。</p>
     *
     * <p><b>AP 模式下本方法不生效</b>：人工切主只对 CP 有意义（AP 本就允许分区期间
     * 短暂双主，"把某机房提为唯一主"在该模式下无从谈起）。AP 下调用被<b>忽略</b>——
     * 仅落一条 {@code op=IGNORED} 审计日志，<b>不改任何状态、不抛异常、不返回错误</b>。</p>
     *
     * <p>每次调用（无论接受/忽略/拒绝）都打一条 {@code PROMOTE-AUDIT} 结构化审计日志，
     * 含时间、操作者、节点、机房、结果、原因、term/epoch 前后值与权重表前后值。</p>
     *
     * @param operator     操作者标识（必填；建议用真实账号/工号，便于审计追溯）
     * @param datacenterId 目标机房标识（必须等于本机房，否则拒绝）
     * @param reason       变更原因（审计）
     * @return true 表示提升已在父组生效并复制出去；false 表示 AP 模式忽略、被闸门拒绝或执行失败
     */
    public static boolean promoteDatacenter(String operator, String datacenterId, String reason) {
        if (instance == null || !instance.running) {
            logger.warn("PROMOTE-AUDIT operator={} target={} reason={} result=REJECTED cause=singleton-not-running",
                PromoteGateway.sanitizeForLog(operator), PromoteGateway.sanitizeForLog(datacenterId), PromoteGateway.sanitizeForLog(reason));
            return false;
        }
        return instance.promoteGateway.doPromoteDatacenter(operator, datacenterId, reason);
    }

    /**
     * 人工回退优先级表到配置派生值（阶段五；与提升同一条复制+持久化+心跳传播路径）。
     *
     * <p>回退<b>不跃升 term、不夺主</b>：只把本机房权重降回配置值，主机房凭更高权重
     * 可在下一次选举夺回，但不自动发生。同样打 {@code PROMOTE-AUDIT} 审计日志。</p>
     *
     * <p><b>AP 模式下本方法同样被忽略</b>（与提升同一条人工切主通路）：仅落
     * {@code op=IGNORED} 审计日志，不改状态、不抛异常。</p>
     *
     * @param operator 操作者标识（必填）
     * @param reason   变更原因（审计）
     * @return true 表示回退已在父组生效并复制出去；false 表示 AP 模式忽略或校验/执行失败
     */
    public static boolean restoreDefaultPriorities(String operator, String reason) {
        if (instance == null || !instance.running) {
            logger.warn("PROMOTE-AUDIT operator={} result=REJECTED cause=singleton-not-running reason={}",
                PromoteGateway.sanitizeForLog(operator), PromoteGateway.sanitizeForLog(reason));
            return false;
        }
        return instance.promoteGateway.doRestoreDefaultPriorities(operator, reason);
    }

    /**
     * 当前父组优先级快照（epoch + 权重表 + 来源）；单机房/子组返回 null。
     * 运维查询用，任意线程可读。
     */
    public static DatacenterPriorityTable.Snapshot getPrioritySnapshot() {
        checkSingletonRunning();
        return instance.parentGroup == null ? null : instance.parentGroup.raftNode.getPrioritySnapshot();
    }


    private static void checkSingletonRunning() {
        if (instance == null || !instance.running) {
            throw new IllegalStateException("Speedboat singleton not running");
        }
    }
}