package cn.itcraft.speedboat.raft;

import cn.itcraft.speedboat.config.SpeedboatConfig;
import cn.itcraft.speedboat.strategy.group.DefaultGroupStrategy;
import cn.itcraft.speedboat.strategy.group.GroupStrategy;
import cn.itcraft.speedboat.transport.NodeEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Raft 组管理器，负责管理一个 Raft 组的完整生命周期。
 * 
 * <p>RaftGroup 是 Raft 集群的抽象表示，包含多个 {@link RaftNode} 实例，提供集群级别的操作接口。
 * 主要用于单进程模拟集群场景，也支持跨机房级联架构。</p>
 * 
 * <p>核心功能：</p>
 * <ul>
 *   <li><b>集群管理</b>：创建、启动、停止组内所有 RaftNode</li>
 *   <li><b>选举协调</b>：跟踪组内 Leader 节点，提供主节点查询接口</li>
 *   <li><b>级联支持</b>：支持父子组关系，实现跨机房双层选举架构</li>
 *   <li><b>配置封装</b>：封装 {@link SpeedboatConfig} 和 {@link GroupStrategy}，简化集群配置</li>
 * </ul>
 * 
 * <p>架构模式：</p>
 * <ol>
 *   <li><b>单机房扁平模式</b>：单个 RaftGroup 管理同一机房内的所有节点</li>
 *   <li><b>跨机房级联模式</b>：多个 RaftGroup 形成父子关系，机房内选举 → 机房间选举</li>
 * </ol>
 * 
 * <p>使用场景：</p>
 * <ul>
 *   <li><b>测试/模拟</b>：在单 JVM 内模拟完整 Raft 集群，便于单元测试和集成测试</li>
 *   <li><b>单进程多节点</b>：特殊场景下需要单进程运行多个 Raft 节点</li>
 *   <li><b>级联架构基类</b>：为跨机房选举提供基础抽象</li>
 * </ul>
 * 
 * <p>注意事项：</p>
 * <ul>
 *   <li>RaftGroup 在单进程内模拟集群，不适用于真实分布式部署</li>
 *   <li>真实分布式场景应使用 {@link RaftNode} 直接构建，每个进程一个节点</li>
 *   <li>父子组关系通过 {@link GroupStrategy} 实现，默认使用 {@link DefaultGroupStrategy}</li>
 * </ul>
 * 
 * @author speedboat
 * @see RaftNode
 * @see GroupStrategy
 * @see SpeedboatConfig
 * @since 1.0.0
 */
public class RaftGroup {

    private static final Logger logger = LoggerFactory.getLogger(RaftGroup.class);

    private final String groupId;
    private final List<RaftNode> nodes;
    private volatile RaftNode leader;

    private final GroupStrategy groupStrategy;
    private final SpeedboatConfig config;

    private RaftGroup parentGroup;
    private final List<RaftGroup> childGroups;

    private volatile boolean running;

    public RaftGroup(String groupId, List<String> nodeUrls, SpeedboatConfig config) {
        this.groupId = Objects.requireNonNull(groupId, "groupId cannot be null");
        this.config = Objects.requireNonNull(config, "config cannot be null");
        this.groupStrategy = resolveGroupStrategy(config);
        this.nodes = createNodes(nodeUrls);
        this.childGroups = new ArrayList<>();
        this.running = false;
        this.leader = null;
    }

    private GroupStrategy resolveGroupStrategy(SpeedboatConfig config) {
        GroupStrategy strategy = config.getGroupStrategy();
        if (strategy != null) {
            return strategy;
        }
        return new DefaultGroupStrategy();
    }

    private List<RaftNode> createNodes(List<String> nodeUrls) {
        if (nodeUrls == null || nodeUrls.isEmpty()) {
            return new ArrayList<>();
        }

        List<RaftNode> createdNodes = new ArrayList<>();
        List<String> peerIds = new ArrayList<>();

        for (String nodeUrl : nodeUrls) {
            NodeEndpoint endpoint = NodeEndpoint.parse(nodeUrl);
            peerIds.add(endpoint.getNodeId());
        }

        for (String nodeUrl : nodeUrls) {
            NodeEndpoint endpoint = NodeEndpoint.parse(nodeUrl);
            String nodeId = endpoint.getNodeId();

            List<String> peersForNode = new ArrayList<>(peerIds);
            peersForNode.remove(nodeId);

            RaftNode node = new RaftNode.Builder()
                .nodeId(nodeId)
                .peerIds(peersForNode)
                .electionTimeout(new ElectionTimeout(
                    groupStrategy.getMinElectionTimeout(),
                    groupStrategy.getMaxElectionTimeout()
                ))
                .groupStrategy(groupStrategy)
                .build();

            createdNodes.add(node);
        }

        return createdNodes;
    }

    public void start() {
        if (running) {
            return;
        }

        running = true;
        for (RaftNode node : nodes) {
            node.start();
        }

        logger.info("RaftGroup {} started with {} nodes", groupId, nodes.size());
    }

    public void shutdown() {
        if (!running) {
            return;
        }

        running = false;
        for (RaftNode node : nodes) {
            node.shutdown();
        }

        logger.info("RaftGroup {} shutdown", groupId);
    }

    public void electLeader() {
        for (RaftNode node : nodes) {
            if (node.isMain()) {
                leader = node;
                logger.info("RaftGroup {} leader elected: {}", groupId, node.getNodeId());
                return;
            }
        }
    }

    public RaftNode getLeader() {
        return leader;
    }

    public boolean isMain() {
        if (leader != null && leader.isMain()) {
            return true;
        }
        for (RaftNode node : nodes) {
            if (node.isMain()) {
                leader = node;
                return true;
            }
        }
        return false;
    }

    public void setParentGroup(RaftGroup parentGroup) {
        this.parentGroup = parentGroup;
        if (parentGroup != null && !parentGroup.childGroups.contains(this)) {
            parentGroup.childGroups.add(this);
        }
    }

    public void addChildGroup(RaftGroup childGroup) {
        if (childGroup != null && !childGroups.contains(childGroup)) {
            childGroups.add(childGroup);
            childGroup.parentGroup = this;
        }
    }

    public RaftGroup getParentGroup() {
        return parentGroup;
    }

    public List<RaftGroup> getChildGroups() {
        return Collections.unmodifiableList(childGroups);
    }

    public void notifyParentLeaderElected(RaftNode childLeader) {
        if (parentGroup == null) {
            return;
        }

        logger.info("Child group {} leader {} elected, notifying parent group {}",
            groupId, childLeader.getNodeId(), parentGroup.getGroupId());
    }

    public boolean checkChildGroupHealth() {
        if (childGroups.isEmpty()) {
            return true;
        }

        for (RaftGroup childGroup : childGroups) {
            if (!childGroup.isRunning()) {
                logger.warn("Child group {} is not running", childGroup.getGroupId());
                return false;
            }
        }

        return true;
    }

    public String getGroupId() {
        return groupId;
    }

    public List<RaftNode> getNodes() {
        return Collections.unmodifiableList(nodes);
    }

    public GroupStrategy getGroupStrategy() {
        return groupStrategy;
    }

    public boolean isRunning() {
        return running;
    }
}
