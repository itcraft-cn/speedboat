package cn.itcraft.speedboat;

import cn.itcraft.speedboat.raft.NodeState;

/**
 * 节点信息封装类，包含 Raft 节点的核心元数据。
 * 
 * <p>NodeInfo 用于在集群中传递节点状态信息，支持监控、诊断和集群管理功能。
 * 包含节点标识、数据中心、状态和任期等关键信息。</p>
 * 
 * <p>主要字段：</p>
 * <ul>
 *   <li><b>nodeId</b>：节点唯一标识，格式为 "hostname-ip" 或自定义标识</li>
 *   <li><b>datacenter</b>：节点所在的数据中心标识，用于跨机房拓扑感知</li>
 *   <li><b>state</b>：节点当前状态（{@link NodeState#FOLLOWER}、{@link NodeState#CANDIDATE}、{@link NodeState#LEADER}）</li>
 *   <li><b>term</b>：节点当前任期号，用于选举安全性保证</li>
 * </ul>
 * 
 * <p>使用场景：</p>
 * <ol>
 *   <li><b>集群监控</b>：获取集群中所有节点的实时状态</li>
 *   <li><b>故障诊断</b>：分析节点状态异常和任期冲突</li>
 *   <li><b>拓扑展示</b>：可视化集群节点分布和状态</li>
 *   <li><b>负载均衡</b>：基于节点状态和位置进行请求路由</li>
 * </ol>
 * 
 * <p>序列化支持：</p>
 * <ul>
 *   <li>NodeInfo 可被 Protostuff 等序列化框架序列化</li>
 *   <li>用于网络传输和持久化存储</li>
 *   <li>支持 JSON 格式输出（通过 toString() 或自定义序列化）</li>
 * </ul>
 * 
 * <p>不变性保证：</p>
 * <ul>
 *   <li>NodeInfo 是不可变对象，所有字段均为 final</li>
 *   <li>线程安全，可被多线程共享访问</li>
 *   <li>hashCode/equals 基于 nodeId 和 term 实现</li>
 * </ul>
 * 
 * <p>示例用法：</p>
 * <pre>{@code
 * // 创建节点信息
 * NodeInfo info = new NodeInfo("node-1", "hangzhou001", NodeState.LEADER, 5);
 * 
 * // 获取节点状态
 * if (info.getState() == NodeState.LEADER) {
 *     System.out.println("节点 " + info.getNodeId() + " 是 Leader，任期 " + info.getTerm());
 * }
 * 
 * // 比较节点信息
 * NodeInfo other = new NodeInfo("node-2", "hangzhou001", NodeState.FOLLOWER, 5);
 * if (info.getDatacenter().equals(other.getDatacenter())) {
 *     System.out.println("两个节点在同一数据中心");
 * }
 * }</pre>
 * 
 * @author speedboat
 * @see NodeState
 * @since 1.0.0
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
