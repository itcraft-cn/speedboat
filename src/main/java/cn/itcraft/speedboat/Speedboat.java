package cn.itcraft.speedboat;

import cn.itcraft.speedboat.config.SpeedboatConfigProvider;
import cn.itcraft.speedboat.raft.ElectionTimeout;
import cn.itcraft.speedboat.raft.RaftGroup;
import cn.itcraft.speedboat.raft.RaftNode;
import cn.itcraft.speedboat.serialize.CustomSerializer;
import cn.itcraft.speedboat.serialize.ProtostuffSerializer;
import cn.itcraft.speedboat.transport.NettyTransport;
import cn.itcraft.speedboat.transport.NodeEndpoint;
import cn.itcraft.speedboat.util.NetworkUtils;
import cn.itcraft.speedboat.lock.DistributedLock;
import cn.itcraft.speedboat.lock.DistributedLockImpl;
import cn.itcraft.speedboat.lock.LockStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
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
            .build();
        
        logger.info("Starting NettyTransport...");
        nettyTransport.start();
        
        logger.info("Starting RaftNode...");
        raftNode.start();
    }
    
    private void doStartCrossDatacenterMode() {
        logger.info("Starting in cross-datacenter (cascade) mode...");
        
        List<List<String>> allNodes = config.getNodes();
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
            .stateMachine(lockStateMachine)
            .build();
        
        logger.info("Starting NettyTransport (intra-datacenter)...");
        nettyTransport.start();
        
        logger.info("Starting RaftNode (intra-datacenter group)...");
        raftNode.start();
        
        logger.info("Cross-datacenter cascade mode initialized (inter-datacenter election will use cross timeout)");
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
        
        if (raftNode != null) {
            raftNode.shutdown();
        }
        
        if (nettyTransport != null) {
            nettyTransport.shutdown();
        }
        
        running = false;
        logger.info("Speedboat stopped");
    }
    
    private boolean checkIsMain() {
        checkRunning();
        return raftNode != null && raftNode.isLeader();
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
            new DistributedLockImpl(name, nodeId, raftNode, lockStateMachine)
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
    
    private String resolveDatacenterId(SpeedboatConfigProvider config, int datacenterIndex) {
        String configuredDcId = config.getDatacenter();
        
        if (configuredDcId != null) {
            return configuredDcId;
        }
        
        return "dc-" + datacenterIndex;
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