package cn.itcraft.speedboat;

import cn.itcraft.speedboat.raft.NodeState;

/**
 * 节点信息
 */
public class NodeInfo {
    
    private final String nodeId;
    private final String datacenter;
    private final NodeState state;
    private final long term;
    
    public NodeInfo(String nodeId, String datacenter, NodeState state, long term) {
        this.nodeId = nodeId;
        this.datacenter = datacenter;
        this.state = state;
        this.term = term;
    }
    
    public String getNodeId() {
        return nodeId;
    }
    
    public String getDatacenter() {
        return datacenter;
    }
    
    public NodeState getState() {
        return state;
    }
    
    public long getTerm() {
        return term;
    }
    
    @Override
    public String toString() {
        return "NodeInfo{"
            + "nodeId='" + nodeId + '\''
            + ", datacenter='" + datacenter + '\''
            + ", state=" + state
            + ", term=" + term
            + '}';
    }
}
