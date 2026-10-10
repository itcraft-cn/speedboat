package cn.itcraft.speedboat.transport;

/**
 * NodeEndpoint 类。
 * 
 * @author speedboat
 * @since 1.0.0
 */
public class NodeEndpoint {
    
    private final String nodeId;
    private final String host;
    private final int port;
    
    public NodeEndpoint(String nodeId, String host, int port) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
    }
    
    public String getNodeId() { return nodeId; }
    public String getHost() { return host; }
    public int getPort() { return port; }
    
    /**
     * 解析 ip:port 形式的节点条目。
     *
     * <p>nodeId 与主路径 {@code NetworkUtils.generateNodeId(ip:port)} 统一——
     * 原实现 nodeId 与 host 都置 host（外部点评修复），与门面主路径两套身份体系；
     * 本方法仅供 {@code RaftGroup}（单进程模拟工具，非生产路径）与测试使用。</p>
     */
    public static NodeEndpoint parse(String nodeUrl) {
        String[] parts = nodeUrl.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        return new NodeEndpoint(
            cn.itcraft.speedboat.util.NetworkUtils.generateNodeId(nodeUrl), host, port);
    }
}
