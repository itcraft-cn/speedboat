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
 * Raft 组管理器
 * 
 * <p>管理单个 Raft 组的生命周期，包含多个 RaftNode 和相关的配置。</p>
 * <p>支持级联架构，可以设置父子组关系。</p>
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
