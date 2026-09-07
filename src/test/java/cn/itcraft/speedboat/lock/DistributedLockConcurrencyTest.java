package cn.itcraft.speedboat.lock;

import cn.itcraft.speedboat.integration.TestNode;
import cn.itcraft.speedboat.raft.ElectionTimeout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("分布式锁并发安全性测试")
class DistributedLockConcurrencyTest {

    private List<TestNode> nodes;
    private List<LockStateMachine> stateMachines;

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
        assertNotNull(findLeader(), "Leader 应在500ms内选出");

        for (LockStateMachine sm : stateMachines) {
            sm.getLockTable().clear();
        }
    }

    @AfterEach
    void tearDown() {
        for (TestNode node : nodes) {
            node.shutdown();
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

    private int findNodeIndex(String id) {
        for (int i = 0; i < nodes.size(); i++) {
            if (nodes.get(i).getNodeId().equals(id)) {
                return i;
            }
        }
        return -1;
    }

    // ========== 互斥性测试 ==========

    @Test
    @DisplayName("多线程并发获取同一锁：只有一个成功")
    void testMutualExclusion() throws InterruptedException {
        String leaderId = findLeader();
        assertNotNull(leaderId, "应选出 Leader");

        TestNode leaderNode = findNode(leaderId);
        int leaderIdx = findNodeIndex(leaderId);

        int threadCount = 10;
        AtomicInteger successCount = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        LockHandle[] handles = new LockHandle[threadCount];

        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            // 使用不同 nodeId 模拟不同节点并发竞争
            final String threadNodeId = "thread-" + i;
            new Thread(() -> {
                try {
                    startLatch.await();
                    DistributedLock lock = new DistributedLockImpl(
                        "mutex-lock", threadNodeId, leaderNode.getRaftNode(), stateMachines.get(leaderIdx)
                    );
                    handles[idx] = lock.tryLock(2000);
                    if (handles[idx] != null && handles[idx].isSuccess()) {
                        successCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(15, TimeUnit.SECONDS), "所有线程应在15秒内完成");

        // 验证互斥性：只有一个线程获取锁成功
        assertEquals(1, successCount.get(), "只有一个线程应获取锁成功");

        // 清理
        for (LockHandle h : handles) {
            if (h != null && h.isSuccess()) {
                h.close();
            }
        }
    }

    @Test
    @DisplayName("锁释放后其他线程能获取")
    void testLockAvailableAfterRelease() throws InterruptedException {
        String leaderId = findLeader();
        assertNotNull(leaderId, "应选出 Leader");

        TestNode leaderNode = findNode(leaderId);
        int leaderIdx = findNodeIndex(leaderId);

        // 线程1获取锁
        DistributedLock lock1 = new DistributedLockImpl(
            "release-test", leaderId, leaderNode.getRaftNode(), stateMachines.get(leaderIdx)
        );
        LockHandle h1 = lock1.tryLock(2000);
        assertTrue(h1.isSuccess(), "线程1应获取锁");

        // 释放锁
        h1.close();
        TimeUnit.MILLISECONDS.sleep(300);

        // 线程2获取锁
        DistributedLock lock2 = new DistributedLockImpl(
            "release-test", leaderId, leaderNode.getRaftNode(), stateMachines.get(leaderIdx)
        );
        try (LockHandle h2 = lock2.tryLock(2000)) {
            assertTrue(h2.isSuccess(), "锁释放后线程2应能获取");
        }
    }

    @Test
    @DisplayName("tryLock 超时后返回失败（不同节点）")
    void testTryLockTimeout() throws InterruptedException {
        String leaderId = findLeader();
        assertNotNull(leaderId, "应选出 Leader");

        TestNode leaderNode = findNode(leaderId);
        int leaderIdx = findNodeIndex(leaderId);

        // 先获取锁
        DistributedLock lock1 = new DistributedLockImpl(
            "timeout-test", leaderId, leaderNode.getRaftNode(), stateMachines.get(leaderIdx)
        );
        LockHandle h1 = lock1.tryLock(2000);
        assertTrue(h1.isSuccess());

        // 另一个不同节点尝试获取，应超时
        String otherNodeId = leaderId.equals("node1") ? "node2" : "node1";
        DistributedLock lock2 = new DistributedLockImpl(
            "timeout-test", otherNodeId, leaderNode.getRaftNode(), stateMachines.get(leaderIdx)
        );
        LockHandle h2 = lock2.tryLock(500);
        assertFalse(h2.isSuccess(), "锁已被其他节点持有时 tryLock 应超时返回 false");

        h1.close();
    }

    @Test
    @DisplayName("不同锁名可并发获取")
    void testDifferentLockNamesConcurrent() throws InterruptedException {
        String leaderId = findLeader();
        assertNotNull(leaderId, "应选出 Leader");

        TestNode leaderNode = findNode(leaderId);
        int leaderIdx = findNodeIndex(leaderId);

        DistributedLock lock1 = new DistributedLockImpl(
            "lock-A", leaderId, leaderNode.getRaftNode(), stateMachines.get(leaderIdx)
        );
        DistributedLock lock2 = new DistributedLockImpl(
            "lock-B", leaderId, leaderNode.getRaftNode(), stateMachines.get(leaderIdx)
        );

        LockHandle h1 = lock1.tryLock(2000);
        LockHandle h2 = lock2.tryLock(2000);

        assertTrue(h1.isSuccess(), "lock-A 应获取成功");
        assertTrue(h2.isSuccess(), "lock-B 应获取成功");
        assertTrue(lock1.isLocked(), "lock-A 应被持有");
        assertTrue(lock2.isLocked(), "lock-B 应被持有");

        h1.close();
        assertFalse(lock1.isLocked(), "lock-A 应被释放");
        assertTrue(lock2.isLocked(), "lock-B 仍应被持有");

        h2.close();
        assertFalse(lock2.isLocked(), "lock-B 应被释放");
    }

    @Test
    @DisplayName("快速连续获取释放不导致状态异常")
    void testRapidAcquireRelease() throws InterruptedException {
        String leaderId = findLeader();
        assertNotNull(leaderId, "应选出 Leader");

        TestNode leaderNode = findNode(leaderId);
        int leaderIdx = findNodeIndex(leaderId);

        for (int i = 0; i < 5; i++) {
            // 每次使用新实例，避免 renewExecutor 已关闭
            DistributedLock lock = new DistributedLockImpl(
                "rapid-test", leaderId, leaderNode.getRaftNode(), stateMachines.get(leaderIdx)
            );
            LockHandle h = lock.tryLock(2000);
            assertTrue(h.isSuccess(), "第" + (i + 1) + "次获取应成功");
            h.close();
            TimeUnit.MILLISECONDS.sleep(50);
        }

        // 最终锁表应为空（所有锁已释放）
        LockStateMachine sm = stateMachines.get(leaderIdx);
        assertFalse(sm.isLockHeldBy("rapid-test", leaderId), "最终锁应被释放");
    }
}
