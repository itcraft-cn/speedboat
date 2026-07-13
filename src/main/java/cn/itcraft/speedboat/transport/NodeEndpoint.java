package cn.itcraft.speedboat.transport;

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
    
    public static NodeEndpoint parse(String nodeUrl) {
        String[] parts = nodeUrl.split(":");
        return new NodeEndpoint(parts[0], parts[0], Integer.parseInt(parts[1]));
    }
}
