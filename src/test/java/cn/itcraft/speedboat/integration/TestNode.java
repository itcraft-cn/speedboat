package cn.itcraft.speedboat.integration;

import cn.itcraft.speedboat.raft.ElectionTimeout;
import cn.itcraft.speedboat.raft.RaftNode;
import cn.itcraft.speedboat.raft.NodeState;
import cn.itcraft.speedboat.strategy.group.GroupStrategy;
import cn.itcraft.speedboat.strategy.voteweight.VoteWeightStrategy;

import java.util.ArrayList;
import java.util.List;

public class TestNode {

    private final RaftNode raftNode;
    private final MockTransport transport;
    private final String nodeId;
    private final List<String> peerIds;

    public TestNode(String nodeId, List<String> peerIds, 
                    ElectionTimeout timeout, 
                    VoteWeightStrategy voteWeightStrategy,
                    GroupStrategy groupStrategy) {
        this.nodeId = nodeId;
        this.peerIds = new ArrayList<>(peerIds);
        this.transport = new MockTransport();
        
        this.raftNode = new RaftNode.Builder()
            .nodeId(nodeId)
            .peerIds(peerIds)
            .electionTimeout(timeout)
            .voteWeightStrategy(voteWeightStrategy)
            .groupStrategy(groupStrategy)
            .transportLayer(transport)
            .build();
    }

    public void start() {
        raftNode.start();
    }

    public void shutdown() {
        raftNode.shutdown();
        transport.clear();
    }

    public MockTransport getTransport() {
        return transport;
    }

    public RaftNode getRaftNode() {
        return raftNode;
    }

    public String getNodeId() {
        return nodeId;
    }

    public List<String> getPeerIds() {
        return peerIds;
    }

    public boolean isLeader() {
        return raftNode.getCurrentState() == NodeState.LEADER;
    }

    public boolean isFollower() {
        return raftNode.getCurrentState() == NodeState.FOLLOWER;
    }

    public boolean isCandidate() {
        return raftNode.getCurrentState() == NodeState.CANDIDATE;
    }

    public long getCurrentTerm() {
        return raftNode.getTerm().getCurrent();
    }

    public String getLeaderId() {
        return raftNode.getLeaderId();
    }

    public void connectTo(TestNode otherNode) {
        transport.registerNode(otherNode.getNodeId(), otherNode.getTransport());
    }

    public void disconnectFrom(String nodeId) {
        transport.unregisterNode(nodeId);
    }

    public void connectToAll(List<TestNode> nodes) {
        for (TestNode node : nodes) {
            if (!node.getNodeId().equals(nodeId)) {
                transport.registerNode(node.getNodeId(), node.getTransport());
            }
        }
    }
}