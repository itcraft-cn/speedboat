package cn.itcraft.speedboat.sample;

import cn.itcraft.speedboat.raft.ElectionTimeout;
import cn.itcraft.speedboat.raft.RaftNode;
import cn.itcraft.speedboat.serialize.CustomSerializer;
import cn.itcraft.speedboat.serialize.ProtostuffSerializer;
import cn.itcraft.speedboat.strategy.group.DefaultGroupStrategy;
import cn.itcraft.speedboat.transport.NettyTransport;
import cn.itcraft.speedboat.transport.NodeEndpoint;

import java.util.ArrayList;
import java.util.List;

/**
 * Speedboat 集群测试单节点
 * 
 * 由 ClusterFailoverTest 通过 mvn exec:java 启动
 * 
 * 参数格式：
 * --nodeId node-1 --port 21001 --peers node-2:127.0.0.1:21002,node-3:127.0.0.1:21003
 */
public class ClusterTestNode {
    
    public static void main(String[] args) throws Exception {
        String nodeId = null;
        int port = 21001;
        String peers = "";
        
        for (int i = 0; i < args.length; i++) {
            if ("--nodeId".equals(args[i]) && i + 1 < args.length) {
                nodeId = args[i + 1];
            }
            if ("--port".equals(args[i]) && i + 1 < args.length) {
                port = Integer.parseInt(args[i + 1]);
            }
            if ("--peers".equals(args[i]) && i + 1 < args.length) {
                peers = args[i + 1];
            }
        }
        
        if (nodeId == null) {
            System.err.println("错误：缺少 --nodeId 参数");
            System.exit(1);
        }
        
        String host = "127.0.0.1";
        
        System.out.println("=================================");
        System.out.println("Speedboat 集群测试节点");
        System.out.println("节点ID: " + nodeId);
        System.out.println("地址: " + host + ":" + port);
        System.out.println("其他节点: " + peers);
        System.out.println("=================================");
        
        NodeEndpoint localEndpoint = new NodeEndpoint(nodeId, host, port);
        
        List<NodeEndpoint> peerEndpoints = new ArrayList<>();
        List<String> peerIds = new ArrayList<>();
        if (!peers.isEmpty()) {
            String[] peerArray = peers.split(",");
            for (String peer : peerArray) {
                String[] parts = peer.split(":");
                if (parts.length >= 3) {
                    String peerId = parts[0];
                    String peerHost = parts[1];
                    int peerPort = Integer.parseInt(parts[2]);
                    peerEndpoints.add(new NodeEndpoint(peerId, peerHost, peerPort));
                    peerIds.add(peerId);
                }
            }
        }
        
        System.out.println("Peer IDs: " + peerIds);
        
        CustomSerializer serializer = new CustomSerializer(new ProtostuffSerializer());
        NettyTransport transport = new NettyTransport(localEndpoint, peerEndpoints, serializer);
        
        ElectionTimeout electionTimeout = new ElectionTimeout(1000, 2000);
        
        RaftNode node = new RaftNode.Builder()
            .nodeId(nodeId)
            .peerIds(peerIds)
            .electionTimeout(electionTimeout)
            .transportLayer(transport)
            .build();
        
        System.out.println("启动网络传输层...");
        transport.start();
        
        System.out.println("启动 RaftNode...");
        node.start();
        
        final RaftNode finalNode = node;
        final String finalNodeId = nodeId;
        Thread monitorThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(2000);
                    boolean isLeader = finalNode.isLeader();
                    System.out.println("[状态报告] " + finalNodeId + " - " + 
                        (isLeader ? "✅ 主节点 (Leader)" : "从节点 (Follower)"));
                } catch (Exception e) {
                    break;
                }
            }
        });
        monitorThread.setDaemon(true);
        monitorThread.start();
        
        System.out.println("节点已启动，等待选举...");
        
        try {
            while (true) {
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        node.shutdown();
        transport.shutdown();
        System.out.println("节点已停止");
    }
}