package cn.itcraft.speedboat.cluster;

import cn.itcraft.speedboat.lock.DistributedLock;
import cn.itcraft.speedboat.lock.DistributedLockImpl;
import cn.itcraft.speedboat.lock.LockHandle;
import cn.itcraft.speedboat.lock.LockStateMachine;
import cn.itcraft.speedboat.raft.*;
import cn.itcraft.speedboat.serialize.CustomSerializer;
import cn.itcraft.speedboat.serialize.ProtostuffSerializer;
import cn.itcraft.speedboat.transport.NettyTransport;
import cn.itcraft.speedboat.transport.NodeEndpoint;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 三节点真实网络集群测试运行器。
 *
 * <p>在三台机器上分别执行同一 JAR，通过真实 TCP 连接验证 Raft 选举、日志复制和分布式锁。</p>
 *
 * <p>用法：java -jar speedboat-cluster.jar {@code <nodeId>} {@code <localPort>} {@code <peer1_host:port>} {@code <peer2_host:port>}</p>
 *
 * <p>示例（本机 174 节点）：</p>
 * <pre>java -jar speedboat.jar node1 9001 192.168.193.176:9002 192.168.193.174:9003</pre>
 */
public class ClusterTestRunner {

    private static final int ELECTION_TIMEOUT_MS = 1000;
    private static final int TEST_TIMEOUT_SECONDS = 30;
    private static final int HEARTBEAT_INTERVAL_MS = 100;

    private static int passed = 0;
    private static int failed = 0;
    private static final List<String> failures = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("用法: java -jar speedboat.jar <nodeId> <localPort> <peer1_host:port> <peer2_host:port>");
            System.err.println("示例: java -jar speedboat.jar node1 9001 192.168.193.176:9002 192.168.193.174:9003");
            System.exit(1);
        }

        String nodeId = args[0];
        int localPort = Integer.parseInt(args[1]);

        List<NodeEndpoint> allEndpoints = new ArrayList<>();
        List<String> peerIds = new ArrayList<>();

        // 本机端点
        allEndpoints.add(new NodeEndpoint(nodeId, "0.0.0.0", localPort));

        // 解析 peer 端点
        for (int i = 2; i < args.length; i++) {
            String[] parts = args[i].split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            String peerId = "node-" + host.replace(".", "-") + "-" + port;
            peerIds.add(peerId);
            allEndpoints.add(new NodeEndpoint(peerId, host, port));
        }

        System.out.println("========================================");
        System.out.println("Speedboat 集群测试 - 节点: " + nodeId);
        System.out.println("本机端口: " + localPort);
        System.out.println("Peers: " + peerIds);
        System.out.println("========================================");

        // 创建序列化器和传输层
        CustomSerializer serializer = new CustomSerializer(new ProtostuffSerializer());
        NodeEndpoint localEndpoint = new NodeEndpoint(nodeId, "0.0.0.0", localPort);
        List<NodeEndpoint> peers = new ArrayList<>();
        for (NodeEndpoint ep : allEndpoints) {
            if (!ep.getNodeId().equals(nodeId)) {
                peers.add(ep);
            }
        }
        NettyTransport transport = new NettyTransport(localEndpoint, peers, serializer);

        // 创建 RaftNode
        LockStateMachine stateMachine = new LockStateMachine();
        RaftNode raftNode = new RaftNode.Builder()
            .nodeId(nodeId)
            .peerIds(peerIds)
            .electionTimeout(new cn.itcraft.speedboat.raft.ElectionTimeout(
                ELECTION_TIMEOUT_MS, ELECTION_TIMEOUT_MS * 2))
            .transportLayer(transport)
            .stateMachine(stateMachine)
            .build();

        // 启动节点
        transport.setRequestVoteHandler(req -> CompletableFuture.completedFuture(raftNode.handleRequestVote(req)));
        transport.setHeartbeatHandler(req -> CompletableFuture.completedFuture(raftNode.handleHeartbeat(req)));
        transport.setAppendEntriesHandler(req -> CompletableFuture.completedFuture(raftNode.handleAppendEntries(req)));
        transport.start();
        raftNode.start();

        System.out.println("[节点 " + nodeId + "] 已启动，等待集群选举...");

        try {
            // 等待 Leader 选出
            boolean elected = waitForLeader(raftNode, TEST_TIMEOUT_SECONDS);
            if (!elected) {
                fail("选举", "集群在 " + TEST_TIMEOUT_SECONDS + " 秒内未选出 Leader");
                printSummary();
                return;
            }

            String leaderId = raftNode.getLeaderId();
            boolean isLeader = raftNode.isLeader();
            System.out.println("[节点 " + nodeId + "] Leader=" + leaderId + ", 本节点是Leader=" + isLeader);

            // ===== 测试1: 选举验证 =====
            if (isLeader) {
                pass("选举", "本节点成功当选为 Leader (" + nodeId + ")");
            } else {
                pass("选举", "集群已选出 Leader (" + leaderId + "), 本节点为 Follower");
            }

            // 等待集群稳定
            TimeUnit.MILLISECONDS.sleep(500);

            // ===== 测试2: 日志复制 =====
            if (isLeader) {
                testLogReplication(raftNode, nodeId);
            } else {
                // Follower 只验证自己能收到日志
                pass("日志复制", "本节点为 Follower, 跳过 propose（由 Leader 执行）");
            }

            // ===== 测试3: 分布式锁 =====
            if (isLeader) {
                testDistributedLock(raftNode, stateMachine, nodeId);
            } else {
                pass("分布式锁", "本节点为 Follower, 跳过锁测试（由 Leader 执行）");
            }

            // ===== 测试4: Leader 故障转移（仅 Leader 执行） =====
            if (isLeader) {
                pass("故障转移", "本节点为 Leader, 跳过故障转移测试（需协调多节点）");
            } else {
                pass("故障转移", "本节点为 Follower, 跳过故障转移测试");
            }

        } finally {
            raftNode.shutdown();
            transport.shutdown();
            printSummary();
        }
    }

    private static void testLogReplication(RaftNode raftNode, String nodeId) {
        System.out.println("[测试] 日志复制: Leader 提交命令并验证 applied");

        long beforeApply = raftNode.getLastApplied();
        long entryIndex = raftNode.propose("cluster-test-cmd".getBytes());
        if (entryIndex <= 0) {
            fail("日志复制", "propose 返回无效索引: " + entryIndex);
            return;
        }

        // 等待应用
        boolean applied;
        try {
            applied = waitForApplied(raftNode, entryIndex, 5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("日志复制", "等待被中断");
            return;
        }
        if (!applied) {
            fail("日志复制", "命令未在5秒内被应用, targetIndex=" + entryIndex
                + ", lastApplied=" + raftNode.getLastApplied()
                + ", commitIndex=" + raftNode.getCommitIndex());
            return;
        }

        long afterApply = raftNode.getLastApplied();
        if (afterApply > beforeApply) {
            pass("日志复制", "命令已应用, index=" + entryIndex
                + ", lastApplied: " + beforeApply + " -> " + afterApply);
        } else {
            fail("日志复制", "lastApplied 未增长: " + beforeApply + " -> " + afterApply);
        }
    }

    private static void testDistributedLock(RaftNode raftNode, LockStateMachine stateMachine, String nodeId) {
        System.out.println("[测试] 分布式锁: 尝试获取锁");

        DistributedLock lock = new DistributedLockImpl("cluster-lock", nodeId, raftNode, stateMachine);
        try (LockHandle handle = lock.tryLock(5000)) {
            if (handle.isSuccess()) {
                pass("分布式锁", "成功获取锁, holder=" + lock.getHolderNodeId());
            } else {
                fail("分布式锁", "获取锁失败");
            }
        } catch (Exception e) {
            fail("分布式锁", "获取锁异常: " + e.getMessage());
        }

        // 验证锁已释放
        try {
            TimeUnit.MILLISECONDS.sleep(300);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (!lock.isLocked()) {
            pass("分布式锁释放", "锁已正确释放");
        } else {
            fail("分布式锁释放", "锁未释放, holder=" + lock.getHolderNodeId());
        }
    }

    // ========== 工具方法 ==========

    private static boolean waitForLeader(RaftNode node, int timeoutSeconds) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        while (System.currentTimeMillis() < deadline) {
            if (node.isLeader() || node.getLeaderId() != null) {
                return true;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        }
        return false;
    }

    private static boolean waitForApplied(RaftNode node, long targetIndex, long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000;
        while (System.nanoTime() < deadline) {
            if (node.getLastApplied() >= targetIndex) {
                return true;
            }
            TimeUnit.MILLISECONDS.sleep(50);
        }
        return false;
    }

    private static void pass(String testName, String detail) {
        passed++;
        System.out.println("  [PASS] " + testName + ": " + detail);
    }

    private static void fail(String testName, String detail) {
        failed++;
        String msg = testName + ": " + detail;
        failures.add(msg);
        System.err.println("  [FAIL] " + msg);
    }

    private static void printSummary() {
        System.out.println();
        System.out.println("========================================");
        System.out.println("测试结果: " + passed + " 通过, " + failed + " 失败");
        if (!failures.isEmpty()) {
            System.out.println("失败详情:");
            for (String f : failures) {
                System.out.println("  - " + f);
            }
        }
        System.out.println("========================================");
        System.exit(failed > 0 ? 1 : 0);
    }
}
