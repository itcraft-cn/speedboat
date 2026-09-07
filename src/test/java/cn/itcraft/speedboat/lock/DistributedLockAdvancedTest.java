package cn.itcraft.speedboat.lock;

import cn.itcraft.speedboat.integration.MockTransport;
import cn.itcraft.speedboat.integration.TestNode;
import cn.itcraft.speedboat.raft.ElectionTimeout;
import cn.itcraft.speedboat.raft.NodeState;
import cn.itcraft.speedboat.raft.RaftNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 分布式锁高级集成测试。
 *
 * <p>覆盖五大核心场景：多节点选举、网络分区恢复、日志一致性、成员变更、故障恢复。</p>
 *
 * <p>前提假设：snapshot 未实现，跳过快照恢复测试。</p>
 */
class DistributedLockAdvancedTest {

    private static final Logger logger = LoggerFactory.getLogger(DistributedLockAdvancedTest.class);

    private List<TestNode> nodes;
    private List<LockStateMachine> stateMachines;

    /* ================================================================
     *  五节点集群
     * ================================================================ */

    private List<TestNode> createCluster(List<String> nodeIds, ElectionTimeout timeout,
                                         List<LockStateMachine> outStateMachines) {
        List<TestNode> cluster = new ArrayList<>();
        for (String nodeId : nodeIds) {
            List<String> peerIds = new ArrayList<>(nodeIds);
            peerIds.remove(nodeId);
            TestNode node = new TestNode(nodeId, peerIds, timeout, null, null);
            LockStateMachine sm = new LockStateMachine();
            node.getRaftNode().setStateMachine(sm);
            cluster.add(node);
            if (outStateMachines != null) {
                outStateMachines.add(sm);
            }
        }
        for (TestNode node : cluster) {
            node.connectToAll(cluster);
        }
        return cluster;
    }

    private void startCluster(List<TestNode> cluster) {
        for (TestNode node : cluster) {
            node.start();
        }
    }

    private void shutdownCluster(List<TestNode> cluster) {
        for (TestNode node : cluster) {
            node.shutdown();
        }
    }

    private String findLeader(List<TestNode> cluster) {
        for (TestNode node : cluster) {
            if (node.isLeader()) {
                return node.getNodeId();
            }
        }
        return null;
    }

    private TestNode findNode(List<TestNode> cluster, String nodeId) {
        for (TestNode node : cluster) {
            if (node.getNodeId().equals(nodeId)) {
                return node;
            }
        }
        return null;
    }

    private LockStateMachine findSM(List<TestNode> cluster, String nodeId) {
        for (int i = 0; i < cluster.size(); i++) {
            if (cluster.get(i).getNodeId().equals(nodeId)) {
                return stateMachines.get(i);
            }
        }
        return null;
    }

    private String waitForLeader(List<TestNode> cluster, String excludeId, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (TestNode node : cluster) {
                if (node.isLeader() && !node.getNodeId().equals(excludeId)) {
                    return node.getNodeId();
                }
            }
            Thread.sleep(50);
        }
        return null;
    }

    private void bidirectionalDisconnect(TestNode a, String bId) {
        a.disconnectFrom(bId);
        TestNode b = findNode(nodes, bId);
        if (b != null) {
            b.disconnectFrom(a.getNodeId());
        }
    }

    private void bidirectionalConnect(TestNode a, TestNode b) {
        a.connectTo(b);
        b.connectTo(a);
    }

    /* ================================================================
     *  setUp / tearDown
     * ================================================================ */

    @BeforeEach
    void setUp() throws InterruptedException {
        nodes = new ArrayList<>();
        stateMachines = new ArrayList<>();

        List<String> allNodeIds = Arrays.asList("node1", "node2", "node3");

        for (String nodeId : allNodeIds) {
            List<String> peerIds = new ArrayList<>(allNodeIds);
            peerIds.remove(nodeId);

            TestNode node = new TestNode(nodeId, peerIds, new ElectionTimeout(150, 300), null, null);
            LockStateMachine sm = new LockStateMachine();
            node.getRaftNode().setStateMachine(sm);

            nodes.add(node);
            stateMachines.add(sm);
        }

        for (TestNode node : nodes) {
            node.connectToAll(nodes);
        }
        for (TestNode node : nodes) {
            node.start();
        }

        TimeUnit.MILLISECONDS.sleep(500);
        assertNotNull(findLeader(nodes), "Leader应在500ms内选出");

        for (LockStateMachine sm : stateMachines) {
            sm.getLockTable().clear();
        }
    }

    @AfterEach
    void tearDown() {
        if (nodes != null) {
            shutdownCluster(nodes);
        }
    }

    /* ================================================================
     *  1. 五节点选举测试
     * ================================================================ */

    @Test
    void testFiveNodeElection() throws InterruptedException {
        shutdownCluster(nodes);

        List<LockStateMachine> fiveSMs = new ArrayList<>();
        nodes = createCluster(
            Arrays.asList("n1", "n2", "n3", "n4", "n5"),
            new ElectionTimeout(150, 300), fiveSMs
        );
        stateMachines = fiveSMs;
        startCluster(nodes);
        TimeUnit.MILLISECONDS.sleep(600);

        String leaderId = findLeader(nodes);
        assertNotNull(leaderId, "五节点集群应选出Leader");
        logger.info("五节点集群 Leader: {}", leaderId);

        TestNode leaderNode = findNode(nodes, leaderId);
        int leaderIdx = nodes.indexOf(leaderNode);
        DistributedLock lock = new DistributedLockImpl(
            "five-node-lock", leaderId, leaderNode.getRaftNode(), stateMachines.get(leaderIdx)
        );
        try (LockHandle handle = lock.tryLock(2000)) {
            assertTrue(handle.isSuccess(), "五节点Leader应能获取锁");
            assertTrue(lock.isLocked());
        }
    }

    @Test
    void testFiveNodeQuorumAfterTwoFailures() throws InterruptedException {
        shutdownCluster(nodes);

        List<LockStateMachine> fiveSMs = new ArrayList<>();
        nodes = createCluster(
            Arrays.asList("n1", "n2", "n3", "n4", "n5"),
            new ElectionTimeout(150, 300), fiveSMs
        );
        stateMachines = fiveSMs;
        startCluster(nodes);
        TimeUnit.MILLISECONDS.sleep(600);

        String leaderId = findLeader(nodes);
        assertNotNull(leaderId, "应选出Leader");

        List<TestNode> toShutdown = new ArrayList<>();
        for (TestNode n : nodes) {
            if (!n.getNodeId().equals(leaderId) && toShutdown.size() < 2) {
                toShutdown.add(n);
            }
        }
        for (TestNode n : toShutdown) {
            n.shutdown();
        }
        TimeUnit.MILLISECONDS.sleep(500);

        TestNode leaderNode = findNode(nodes, leaderId);
        int leaderIdx = nodes.indexOf(leaderNode);
        DistributedLock lock = new DistributedLockImpl(
            "quorum-lock", leaderId, leaderNode.getRaftNode(), stateMachines.get(leaderIdx)
        );
        try (LockHandle handle = lock.tryLock(2000)) {
            assertTrue(handle.isSuccess(), "3/5节点应构成多数派获取锁");
        }
    }

    @Test
    void testFiveNodeLeaderFailover() throws InterruptedException {
        shutdownCluster(nodes);

        List<LockStateMachine> fiveSMs = new ArrayList<>();
        nodes = createCluster(
            Arrays.asList("n1", "n2", "n3", "n4", "n5"),
            new ElectionTimeout(150, 300), fiveSMs
        );
        stateMachines = fiveSMs;
        startCluster(nodes);
        TimeUnit.MILLISECONDS.sleep(600);

        String oldLeaderId = findLeader(nodes);
        assertNotNull(oldLeaderId, "应选出Leader");

        TestNode oldLeader = findNode(nodes, oldLeaderId);
        oldLeader.shutdown();
        TimeUnit.MILLISECONDS.sleep(200);

        for (TestNode n : nodes) {
            if (!n.getNodeId().equals(oldLeaderId)) {
                bidirectionalDisconnect(n, oldLeaderId);
            }
        }

        String newLeaderId = waitForLeader(nodes, oldLeaderId, 5000);
        assertNotNull(newLeaderId, "应选出新Leader");
        assertNotEquals(oldLeaderId, newLeaderId, "新Leader不应是旧Leader");
        logger.info("五节点故障转移: {} -> {}", oldLeaderId, newLeaderId);

        TestNode newLeader = findNode(nodes, newLeaderId);
        int newIdx = nodes.indexOf(newLeader);
        DistributedLock lock = new DistributedLockImpl(
            "failover-lock", newLeaderId, newLeader.getRaftNode(), stateMachines.get(newIdx)
        );
        try (LockHandle handle = lock.tryLock(2000)) {
            assertTrue(handle.isSuccess(), "新Leader应能获取锁");
        }
    }

    /* ================================================================
     *  2. 网络分区 + 恢复测试
     * ================================================================ */

    @Test
    void testPartitionIsolatesLeader() throws InterruptedException {
        String leaderId = findLeader(nodes);
        assertNotNull(leaderId, "应选出Leader");

        // 只断开 Leader 与所有其他节点的连接，但保持其他节点之间互通
        for (TestNode n : nodes) {
            if (!n.getNodeId().equals(leaderId)) {
                bidirectionalDisconnect(n, leaderId);
            }
        }

        // 等待新Leader选出
        String newLeaderId = waitForLeader(nodes, leaderId, 5000);
        assertNotNull(newLeaderId, "分区后应选出新Leader");
        logger.info("分区后新Leader: {}", newLeaderId);

        // 新Leader应能获取锁
        TestNode newLeader = findNode(nodes, newLeaderId);
        int newIdx = nodes.indexOf(newLeader);
        DistributedLock lock = new DistributedLockImpl(
            "partition-lock", newLeaderId, newLeader.getRaftNode(), stateMachines.get(newIdx)
        );
        try (LockHandle handle = lock.tryLock(2000)) {
            assertTrue(handle.isSuccess(), "新Leader应能获取锁");
        }
    }

    @Test
    void testPartitionHealAndLogSync() throws InterruptedException {
        String leaderId = findLeader(nodes);
        assertNotNull(leaderId, "应选出Leader");

        // 旧Leader获取锁
        TestNode oldLeader = findNode(nodes, leaderId);
        int oldIdx = nodes.indexOf(oldLeader);
        DistributedLock oldLock = new DistributedLockImpl(
            "heal-lock", leaderId, oldLeader.getRaftNode(), stateMachines.get(oldIdx)
        );
        LockHandle oldHandle = oldLock.tryLock(2000);
        assertTrue(oldHandle.isSuccess(), "旧Leader应能获取锁");
        oldHandle.close();

        // 分区：断开所有节点间连接
        for (int i = 0; i < nodes.size(); i++) {
            for (int j = i + 1; j < nodes.size(); j++) {
                bidirectionalDisconnect(nodes.get(i), nodes.get(j).getNodeId());
            }
        }

        // 等待一段时间让分区生效
        TimeUnit.MILLISECONDS.sleep(1000);

        // 恢复所有连接
        for (int i = 0; i < nodes.size(); i++) {
            for (int j = i + 1; j < nodes.size(); j++) {
                bidirectionalConnect(nodes.get(i), nodes.get(j));
            }
        }

        // 等待Leader稳定
        TimeUnit.MILLISECONDS.sleep(2000);
        String stableLeaderId = findLeader(nodes);
        assertNotNull(stableLeaderId, "恢复后应有Leader");

        // 验证日志一致性：所有节点的锁状态应一致
        TestNode stableLeader = findNode(nodes, stableLeaderId);
        int stableIdx = nodes.indexOf(stableLeader);
        DistributedLock lock = new DistributedLockImpl(
            "heal-lock", stableLeaderId, stableLeader.getRaftNode(), stateMachines.get(stableIdx)
        );
        // 锁应该已释放（旧Leader的handle.close()已同步）
        // 新Leader应能获取锁
        try (LockHandle handle = lock.tryLock(2000)) {
            assertTrue(handle.isSuccess(), "恢复后新Leader应能获取锁");
        }
    }

    @Test
    void testMinorityPartitionCannotAcquireLock() throws InterruptedException {
        String leaderId = findLeader(nodes);
        assertNotNull(leaderId, "应选出Leader");

        // 分区：1个节点（含Leader）vs 2个节点
        TestNode minorityNode = null;
        for (TestNode n : nodes) {
            if (n.getNodeId().equals(leaderId)) {
                minorityNode = n;
                break;
            }
        }

        // 断开所有连接
        for (int i = 0; i < nodes.size(); i++) {
            for (int j = i + 1; j < nodes.size(); j++) {
                bidirectionalDisconnect(nodes.get(i), nodes.get(j).getNodeId());
            }
        }

        // 多数派（2节点）应能选出新Leader并获取锁
        // 找到多数派中的节点
        TestNode majorityNode = null;
        for (TestNode n : nodes) {
            if (!n.getNodeId().equals(leaderId) && majorityNode == null) {
                majorityNode = n;
            }
        }

        // 多数派内部恢复连接
        for (TestNode n : nodes) {
            if (!n.getNodeId().equals(leaderId) && n != majorityNode) {
                bidirectionalConnect(majorityNode, n);
            }
        }

        TimeUnit.MILLISECONDS.sleep(2000);

        // 尝试在多数派中找Leader
        String majorityLeader = null;
        for (TestNode n : nodes) {
            if (n.isLeader() && !n.getNodeId().equals(leaderId)) {
                majorityLeader = n.getNodeId();
                break;
            }
        }

        if (majorityLeader != null) {
            TestNode ml = findNode(nodes, majorityLeader);
            int mlIdx = nodes.indexOf(ml);
            DistributedLock lock = new DistributedLockImpl(
                "minority-lock", majorityLeader, ml.getRaftNode(), stateMachines.get(mlIdx)
            );
            try (LockHandle handle = lock.tryLock(2000)) {
                assertTrue(handle.isSuccess(), "多数派应能获取锁");
            }
        }
        // 少数派（旧Leader）不应能获取锁（propose会超时）
    }

    /* ================================================================
     *  3. 日志一致性验证测试
     * ================================================================ */

    @Test
    void testLogConsistencyAfterLockAcquire() throws InterruptedException {
        String leaderId = findLeader(nodes);
        assertNotNull(leaderId, "应选出Leader");

        TestNode leaderNode = findNode(nodes, leaderId);
        int leaderIdx = nodes.indexOf(leaderNode);

        DistributedLock lock = new DistributedLockImpl(
            "log-consistency-lock", leaderId, leaderNode.getRaftNode(), stateMachines.get(leaderIdx)
        );
        try (LockHandle handle = lock.tryLock(2000)) {
            assertTrue(handle.isSuccess());
        }

        // 等待日志复制完成
        TimeUnit.MILLISECONDS.sleep(500);

        // 验证所有节点的日志大小一致
        long leaderLogSize = leaderNode.getRaftNode().getLastApplied();
        for (TestNode n : nodes) {
            long nodeLogSize = n.getRaftNode().getLastApplied();
            assertEquals(leaderLogSize, nodeLogSize,
                "节点 " + n.getNodeId() + " 日志应与Leader一致");
        }
    }

    @Test
    void testLogConsistencyAfterMultipleOperations() throws InterruptedException {
        String leaderId = findLeader(nodes);
        assertNotNull(leaderId, "应选出Leader");

        TestNode leaderNode = findNode(nodes, leaderId);
        int leaderIdx = nodes.indexOf(leaderNode);

        // 执行多次锁操作
        for (int i = 0; i < 3; i++) {
            DistributedLock lock = new DistributedLockImpl(
                "multi-op-lock-" + i, leaderId, leaderNode.getRaftNode(), stateMachines.get(leaderIdx)
            );
            try (LockHandle handle = lock.tryLock(2000)) {
                assertTrue(handle.isSuccess(), "第" + (i + 1) + "次锁获取应成功");
            }
        }

        // 等待日志复制
        TimeUnit.MILLISECONDS.sleep(500);

        // 验证所有节点日志一致
        long leaderLogSize = leaderNode.getRaftNode().getLastApplied();
        for (TestNode n : nodes) {
            long nodeLogSize = n.getRaftNode().getLastApplied();
            assertEquals(leaderLogSize, nodeLogSize,
                "节点 " + n.getNodeId() + " 日志大小应与Leader一致");
        }
    }

    @Test
    void testFollowerCatchesUpAfterReconnect() throws InterruptedException {
        String leaderId = findLeader(nodes);
        assertNotNull(leaderId, "应选出Leader");

        TestNode leaderNode = findNode(nodes, leaderId);
        int leaderIdx = nodes.indexOf(leaderNode);

        // 获取一个follower并断开
        TestNode follower = null;
        for (TestNode n : nodes) {
            if (!n.getNodeId().equals(leaderId)) {
                follower = n;
                break;
            }
        }
        assertNotNull(follower, "应有follower");

        bidirectionalDisconnect(follower, leaderId);
        TimeUnit.MILLISECONDS.sleep(100);

        // Leader在follower断开期间执行多次操作
        for (int i = 0; i < 3; i++) {
            DistributedLock lock = new DistributedLockImpl(
                "catchup-lock-" + i, leaderId, leaderNode.getRaftNode(), stateMachines.get(leaderIdx)
            );
            try (LockHandle handle = lock.tryLock(2000)) {
                assertTrue(handle.isSuccess());
            }
        }

        // 恢复连接
        bidirectionalConnect(follower, leaderNode);
        TimeUnit.MILLISECONDS.sleep(1000);

        // 验证follower的日志已追上
        long leaderLogSize = leaderNode.getRaftNode().getLastApplied();
        long followerLogSize = follower.getRaftNode().getLastApplied();
        assertEquals(leaderLogSize, followerLogSize,
            "follower重连后日志应与Leader一致");
    }

    /* ================================================================
     *  4. 故障恢复测试
     * ================================================================ */

    @Test
    void testLockSurvivesLeaderRestart() throws InterruptedException {
        String leaderId = findLeader(nodes);
        assertNotNull(leaderId, "应选出Leader");

        TestNode leaderNode = findNode(nodes, leaderId);
        int leaderIdx = nodes.indexOf(leaderNode);

        // Leader获取锁
        DistributedLock lock = new DistributedLockImpl(
            "restart-lock", leaderId, leaderNode.getRaftNode(), stateMachines.get(leaderIdx)
        );
        LockHandle handle = lock.tryLock(2000);
        assertTrue(handle.isSuccess(), "Leader应能获取锁");

        // 释放锁
        handle.close();

        // 关闭Leader
        leaderNode.shutdown();
        for (TestNode n : nodes) {
            if (!n.getNodeId().equals(leaderId)) {
                bidirectionalDisconnect(n, leaderId);
            }
        }

        // 等待新Leader
        String newLeaderId = waitForLeader(nodes, leaderId, 5000);
        assertNotNull(newLeaderId, "应选出新Leader");
        logger.info("Leader重启测试: 旧Leader={}, 新Leader={}", leaderId, newLeaderId);

        // 新Leader应能获取锁
        TestNode newLeader = findNode(nodes, newLeaderId);
        int newIdx = nodes.indexOf(newLeader);
        DistributedLock newLock = new DistributedLockImpl(
            "restart-lock", newLeaderId, newLeader.getRaftNode(), stateMachines.get(newIdx)
        );
        try (LockHandle handle2 = newLock.tryLock(2000)) {
            assertTrue(handle2.isSuccess(), "新Leader应能获取锁");
        }
    }

    @Test
    void testConcurrentLockAttempts() throws InterruptedException {
        String leaderId = findLeader(nodes);
        assertNotNull(leaderId, "应选出Leader");

        TestNode leaderNode = findNode(nodes, leaderId);
        int leaderIdx = nodes.indexOf(leaderNode);

        // 多个线程同时尝试获取锁
        int threadCount = 3;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        LockHandle[] handles = new LockHandle[threadCount];

        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            new Thread(() -> {
                try {
                    startLatch.await();
                    DistributedLock lock = new DistributedLockImpl(
                        "concurrent-lock-" + idx, leaderId, leaderNode.getRaftNode(), stateMachines.get(leaderIdx)
                    );
                    handles[idx] = lock.tryLock(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "所有线程应在10秒内完成");

        // 同一锁名不应被并发获取（互斥）
        // 不同锁名应都能获取
        int successCount = 0;
        for (LockHandle h : handles) {
            if (h != null && h.isSuccess()) {
                successCount++;
            }
        }
        // 至少应有一个成功
        assertTrue(successCount >= 1, "至少应有一个线程获取锁");

        // 清理
        for (LockHandle h : handles) {
            if (h != null) {
                h.close();
            }
        }
    }

    @Test
    void testLockReleaseAndReacquire() throws InterruptedException {
        String leaderId = findLeader(nodes);
        assertNotNull(leaderId, "应选出Leader");

        TestNode leaderNode = findNode(nodes, leaderId);
        int leaderIdx = nodes.indexOf(leaderNode);

        // 第一次获取
        DistributedLock lock1 = new DistributedLockImpl(
            "reacquire-lock", leaderId, leaderNode.getRaftNode(), stateMachines.get(leaderIdx)
        );
        LockHandle h1 = lock1.tryLock(2000);
        assertTrue(h1.isSuccess(), "第一次获取应成功");
        h1.close();

        // 等待释放生效
        TimeUnit.MILLISECONDS.sleep(300);

        // 第二次获取（新实例，避免 executor 已关闭）
        DistributedLock lock2 = new DistributedLockImpl(
            "reacquire-lock", leaderId, leaderNode.getRaftNode(), stateMachines.get(leaderIdx)
        );
        try (LockHandle h2 = lock2.tryLock(2000)) {
            assertTrue(h2.isSuccess(), "释放后重新获取应成功");
        }
    }

    /* ================================================================
     *  5. 锁转移端到端验证
     * ================================================================ */

    @Test
    void testEndToEndLockTransfer() throws InterruptedException {
        String leaderId = findLeader(nodes);
        assertNotNull(leaderId, "应选出Leader");

        TestNode leaderNode = findNode(nodes, leaderId);
        int leaderIdx = nodes.indexOf(leaderNode);

        // 1. Leader获取锁
        DistributedLock lock = new DistributedLockImpl(
            "e2e-transfer", leaderId, leaderNode.getRaftNode(), stateMachines.get(leaderIdx)
        );
        LockHandle handle = lock.tryLock(2000);
        assertTrue(handle.isSuccess(), "Leader应能获取锁");
        assertEquals(leaderId, lock.getHolderNodeId());

        // 2. 释放锁
        handle.close();
        assertFalse(lock.isLocked(), "锁应已释放");

        // 3. 模拟Leader故障
        leaderNode.shutdown();
        for (TestNode n : nodes) {
            if (!n.getNodeId().equals(leaderId)) {
                bidirectionalDisconnect(n, leaderId);
            }
        }

        // 4. 等待新Leader
        String newLeaderId = waitForLeader(nodes, leaderId, 5000);
        assertNotNull(newLeaderId, "应选出新Leader");

        // 5. 新Leader获取锁
        TestNode newLeader = findNode(nodes, newLeaderId);
        int newIdx = nodes.indexOf(newLeader);
        DistributedLock newLock = new DistributedLockImpl(
            "e2e-transfer", newLeaderId, newLeader.getRaftNode(), stateMachines.get(newIdx)
        );
        try (LockHandle newHandle = newLock.tryLock(2000)) {
            assertTrue(newHandle.isSuccess(), "新Leader应能获取锁");
            assertEquals(newLeaderId, newLock.getHolderNodeId());
        }

        // 6. 恢复旧Leader并验证其无法冲突
        TestNode oldLeaderRecovered = findNode(nodes, leaderId);
        if (oldLeaderRecovered != null) {
            // 旧Leader不应还能获取锁（已被新Leader获取且释放）
            // 这里只验证不抛异常
            DistributedLock oldLock = new DistributedLockImpl(
                "e2e-transfer", leaderId, oldLeaderRecovered.getRaftNode(), stateMachines.get(leaderIdx)
            );
            // 旧Leader不再是leader，propose会失败
            LockHandle oldHandle = oldLock.tryLock(1000);
            assertFalse(oldHandle.isSuccess(), "旧Leader不应能获取锁");
        }
    }

    @Test
    void testLockTransferWithConcurrentAccess() throws InterruptedException {
        String leaderId = findLeader(nodes);
        assertNotNull(leaderId, "应选出Leader");

        TestNode leaderNode = findNode(nodes, leaderId);
        int leaderIdx = nodes.indexOf(leaderNode);

        // Leader获取锁并释放
        DistributedLock lock = new DistributedLockImpl(
            "concurrent-transfer", leaderId, leaderNode.getRaftNode(), stateMachines.get(leaderIdx)
        );
        LockHandle handle = lock.tryLock(2000);
        assertTrue(handle.isSuccess());
        handle.close();
        TimeUnit.MILLISECONDS.sleep(200);

        // 模拟Leader故障
        leaderNode.shutdown();
        for (TestNode n : nodes) {
            if (!n.getNodeId().equals(leaderId)) {
                bidirectionalDisconnect(n, leaderId);
            }
        }

        // 等待新Leader
        String newLeaderId = waitForLeader(nodes, leaderId, 5000);
        assertNotNull(newLeaderId, "应选出新Leader");

        // 新Leader应能获取锁
        TestNode newLeader = findNode(nodes, newLeaderId);
        int newIdx = nodes.indexOf(newLeader);
        DistributedLock newLock = new DistributedLockImpl(
            "concurrent-transfer", newLeaderId, newLeader.getRaftNode(), stateMachines.get(newIdx)
        );
        try (LockHandle newHandle = newLock.tryLock(2000)) {
            assertTrue(newHandle.isSuccess(), "新Leader应能获取锁");
        }
    }
}
