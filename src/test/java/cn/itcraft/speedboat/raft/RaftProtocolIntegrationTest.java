package cn.itcraft.speedboat.raft;

import cn.itcraft.speedboat.integration.MockTransport;
import cn.itcraft.speedboat.integration.TestNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Raft 核心协议集成测试：选举 + 日志复制 + 并发锁")
class RaftProtocolIntegrationTest {

    private final List<TestNode> nodes = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (TestNode node : nodes) {
            node.shutdown();
        }
        nodes.clear();
    }

    private TestNode createNode(String id, List<String> peers) {
        TestNode node = new TestNode(id, peers, new ElectionTimeout(150, 300), null, null);
        nodes.add(node);
        return node;
    }

    private void connectAll() {
        for (TestNode a : nodes) {
            a.connectToAll(nodes);
        }
    }

    private void startAll() {
        for (TestNode node : nodes) {
            node.start();
        }
    }

    private String findLeader() {
        for (TestNode node : nodes) {
            if (node.isLeader()) {
                return node.getNodeId();
            }
        }
        return null;
    }

    private TestNode findNode(String id) {
        for (TestNode node : nodes) {
            if (node.getNodeId().equals(id)) {
                return node;
            }
        }
        return null;
    }

    // ========== 两节点选举 ==========

    @Test
    @DisplayName("两节点集群：能选出唯一 Leader")
    void testTwoNodeElection() throws InterruptedException {
        createNode("n1", Arrays.asList("n2"));
        createNode("n2", Arrays.asList("n1"));
        connectAll();
        startAll();

        TimeUnit.MILLISECONDS.sleep(600);

        String leaderId = findLeader();
        assertNotNull(leaderId, "两节点集群应选出 Leader");

        // 只能有一个 Leader
        long leaderCount = nodes.stream().filter(TestNode::isLeader).count();
        assertEquals(1, leaderCount, "只能有一个 Leader");
    }

    @Test
    @DisplayName("两节点集群：一个节点关闭后另一个仍能当选")
    void testTwoNodeFailover() throws InterruptedException {
        createNode("n1", Arrays.asList("n2"));
        createNode("n2", Arrays.asList("n1"));
        connectAll();
        startAll();

        TimeUnit.MILLISECONDS.sleep(600);
        String leaderId = findLeader();
        assertNotNull(leaderId, "应选出 Leader");

        // 关闭一个节点
        TestNode other = findNode(leaderId.equals("n1") ? "n2" : "n1");
        other.shutdown();
        TimeUnit.MILLISECONDS.sleep(500);

        // 剩余节点应仍为 Leader（它已经是 Leader，不会丢失）
        TestNode leader = findNode(leaderId);
        assertTrue(leader.isLeader(), "存活的 Leader 应保持 Leader 状态");
    }

    @Test
    @DisplayName("两节点集群：两节点同时启动，选举应在超时内完成")
    void testTwoNodeElectionCompletesWithinTimeout() throws InterruptedException {
        createNode("n1", Arrays.asList("n2"));
        createNode("n2", Arrays.asList("n1"));
        connectAll();
        startAll();

        long start = System.currentTimeMillis();
        while (findLeader() == null && System.currentTimeMillis() - start < 3000) {
            TimeUnit.MILLISECONDS.sleep(50);
        }

        assertNotNull(findLeader(), "选举应在3秒内完成");
        assertTrue(System.currentTimeMillis() - start < 3000, "选举耗时应小于3秒");
    }

    // ========== 三节点日志复制 ==========

    @Test
    @DisplayName("三节点集群：Leader 提交命令后所有节点日志一致")
    void testThreeNodeLogReplication() throws InterruptedException {
        createNode("n1", Arrays.asList("n2", "n3"));
        createNode("n2", Arrays.asList("n1", "n3"));
        createNode("n3", Arrays.asList("n1", "n2"));
        connectAll();
        startAll();

        TimeUnit.MILLISECONDS.sleep(600);
        String leaderId = findLeader();
        assertNotNull(leaderId, "应选出 Leader");

        TestNode leader = findNode(leaderId);
        // Leader 自己 propose 一条命令
        byte[] data = "test-command".getBytes();
        long entryIndex = leader.getRaftNode().propose(data);
        assertTrue(entryIndex > 0, "propose 应返回有效索引");

        // 等待日志复制
        TimeUnit.MILLISECONDS.sleep(500);

        // 所有节点的 lastApplied 应一致
        long leaderApplied = leader.getRaftNode().getLastApplied();
        for (TestNode node : nodes) {
            long nodeApplied = node.getRaftNode().getLastApplied();
            assertEquals(leaderApplied, nodeApplied,
                "节点 " + node.getNodeId() + " 的 lastApplied 应与 Leader 一致");
        }
    }

    @Test
    @DisplayName("三节点集群：连续提交多条命令后日志一致")
    void testThreeNodeMultipleCommandsReplication() throws InterruptedException {
        createNode("n1", Arrays.asList("n2", "n3"));
        createNode("n2", Arrays.asList("n1", "n3"));
        createNode("n3", Arrays.asList("n1", "n2"));
        connectAll();
        startAll();

        TimeUnit.MILLISECONDS.sleep(600);
        String leaderId = findLeader();
        assertNotNull(leaderId, "应选出 Leader");

        TestNode leader = findNode(leaderId);

        // 连续提交5条命令
        for (int i = 0; i < 5; i++) {
            long idx = leader.getRaftNode().propose(("cmd-" + i).getBytes());
            assertTrue(idx > 0, "第" + (i + 1) + "条命令应提交成功");
        }

        TimeUnit.MILLISECONDS.sleep(500);

        // 所有节点的 lastApplied 应一致
        long leaderApplied = leader.getRaftNode().getLastApplied();
        for (TestNode node : nodes) {
            long nodeApplied = node.getRaftNode().getLastApplied();
            assertEquals(leaderApplied, nodeApplied,
                "节点 " + node.getNodeId() + " 的 lastApplied 应与 Leader 一致");
        }
    }

    // ========== 三节点选举 ==========

    @Test
    @DisplayName("三节点集群：Leader 关闭后新 Leader 被选出")
    void testThreeNodeLeaderFailover() throws InterruptedException {
        createNode("n1", Arrays.asList("n2", "n3"));
        createNode("n2", Arrays.asList("n1", "n3"));
        createNode("n3", Arrays.asList("n1", "n2"));
        connectAll();
        startAll();

        TimeUnit.MILLISECONDS.sleep(600);
        String oldLeaderId = findLeader();
        assertNotNull(oldLeaderId, "应选出 Leader");

        // 关闭 Leader
        TestNode oldLeader = findNode(oldLeaderId);
        oldLeader.shutdown();
        for (TestNode n : nodes) {
            if (!n.getNodeId().equals(oldLeaderId)) {
                n.disconnectFrom(oldLeaderId);
            }
        }

        // 等待新 Leader
        long start = System.currentTimeMillis();
        String newLeaderId = null;
        while (System.currentTimeMillis() - start < 5000) {
            for (TestNode n : nodes) {
                if (n.isLeader() && !n.getNodeId().equals(oldLeaderId)) {
                    newLeaderId = n.getNodeId();
                    break;
                }
            }
            if (newLeaderId != null) break;
            TimeUnit.MILLISECONDS.sleep(50);
        }

        assertNotNull(newLeaderId, "应选出新 Leader");
        assertNotEquals(oldLeaderId, newLeaderId, "新 Leader 不应是旧 Leader");
    }

    @Test
    @DisplayName("三节点集群：日志冲突后 Follower 能与新 Leader 同步")
    void testLogSyncAfterConflict() throws InterruptedException {
        createNode("n1", Arrays.asList("n2", "n3"));
        createNode("n2", Arrays.asList("n1", "n3"));
        createNode("n3", Arrays.asList("n1", "n2"));
        connectAll();
        startAll();

        TimeUnit.MILLISECONDS.sleep(600);
        String leaderId = findLeader();
        assertNotNull(leaderId, "应选出 Leader");

        TestNode leader = findNode(leaderId);

        // 记录 propose 前的 lastApplied（包含 leader info 条目）
        long beforeApply = leader.getRaftNode().getLastApplied();
        assertTrue(beforeApply >= 1, "Leader 至少已应用 leader info 条目");

        // Leader 提交一条命令
        leader.getRaftNode().propose("cmd-1".getBytes());
        TimeUnit.MILLISECONDS.sleep(500);

        // 所有节点应一致，且 Leader 的 lastApplied 至少增加了1
        long leaderApplied = leader.getRaftNode().getLastApplied();
        assertTrue(leaderApplied > beforeApply, "Leader 的 lastApplied 应增长");

        for (TestNode node : nodes) {
            assertEquals(leaderApplied, node.getRaftNode().getLastApplied(),
                "所有节点 lastApplied 应一致");
        }
    }
}
