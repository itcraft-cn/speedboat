package cn.itcraft.speedboat.lock;

import cn.itcraft.speedboat.integration.MockTransport;
import cn.itcraft.speedboat.integration.TestNode;
import cn.itcraft.speedboat.raft.ElectionTimeout;
import cn.itcraft.speedboat.raft.NodeState;
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

class DistributedLockIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(DistributedLockIntegrationTest.class);

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

            ElectionTimeout timeout = new ElectionTimeout(150, 300);
            TestNode node = new TestNode(nodeId, peerIds, timeout, null, null);

            LockStateMachine stateMachine = new LockStateMachine();
            node.getRaftNode().setStateMachine(stateMachine);

            nodes.add(node);
            stateMachines.add(stateMachine);
        }

        for (TestNode node : nodes) {
            node.connectToAll(nodes);
        }

        for (TestNode node : nodes) {
            node.start();
        }

        TimeUnit.MILLISECONDS.sleep(500);

        String currentLeaderNodeId = getCurrentLeaderNodeId();
        logger.info("setUp completed, leader={}", currentLeaderNodeId);
        assertNotNull(currentLeaderNodeId, "Leader should be elected within 500ms");

        for (LockStateMachine sm : stateMachines) {
            sm.getLockTable().clear();
        }
        logger.info("setUp: cleared all lock tables");
    }

    private String getCurrentLeaderNodeId() {
        for (TestNode node : nodes) {
            if (node.isLeader()) {
                logger.info("Leader elected: {} (Thread: {})", node.getNodeId(), Thread.currentThread().getName());
                return node.getNodeId();
            }
        }
        return null;
    }

    @AfterEach
    void tearDown() {
        for (TestNode node : nodes) {
            node.shutdown();
        }
    }

    @Test
    void testLeaderCanAcquireLock() {
        String currentLeaderNodeId = getCurrentLeaderNodeId();
        TestNode leaderNode = findNodeById(currentLeaderNodeId);
        LockStateMachine leaderStateMachine = findStateMachineByNodeId(currentLeaderNodeId);

        DistributedLock lock = new DistributedLockImpl(
            "test-lock", currentLeaderNodeId, leaderNode.getRaftNode(), leaderStateMachine
        );

        LockHandle handle = lock.tryLock(2000);

        assertTrue(handle.isSuccess(), "Leader should be able to acquire lock");
        assertEquals("test-lock", handle.getLockName());
        assertEquals(currentLeaderNodeId, handle.getNodeId());

        assertTrue(lock.isLocked());
        assertTrue(lock.isHeldByCurrentNode());
        assertEquals(currentLeaderNodeId, lock.getHolderNodeId());

        handle.close();

        assertFalse(lock.isLocked());
    }

    @Test
    void testTryWithResources() {
        String currentLeaderNodeId = getCurrentLeaderNodeId();
        TestNode leaderNode = findNodeById(currentLeaderNodeId);
        LockStateMachine leaderStateMachine = findStateMachineByNodeId(currentLeaderNodeId);

        DistributedLock lock = new DistributedLockImpl(
            "test-lock-2", currentLeaderNodeId, leaderNode.getRaftNode(), leaderStateMachine
        );

        try (LockHandle handle = lock.tryLock(2000)) {
            assertTrue(handle.isSuccess());
            assertTrue(lock.isLocked());
        }

        assertFalse(lock.isLocked());
    }

    @Test
    void testMultipleLocks() {
        String currentLeaderNodeId = getCurrentLeaderNodeId();
        TestNode leaderNode = findNodeById(currentLeaderNodeId);
        LockStateMachine leaderStateMachine = findStateMachineByNodeId(currentLeaderNodeId);

        DistributedLock lock1 = new DistributedLockImpl(
            "lock-1", currentLeaderNodeId, leaderNode.getRaftNode(), leaderStateMachine
        );
        DistributedLock lock2 = new DistributedLockImpl(
            "lock-2", currentLeaderNodeId, leaderNode.getRaftNode(), leaderStateMachine
        );

        LockHandle handle1 = lock1.tryLock(2000);
        LockHandle handle2 = lock2.tryLock(2000);

        assertTrue(handle1.isSuccess());
        assertTrue(handle2.isSuccess());
        assertTrue(lock1.isLocked());
        assertTrue(lock2.isLocked());

        handle1.close();
        handle2.close();

        assertFalse(lock1.isLocked());
        assertFalse(lock2.isLocked());
    }

    @Test
    void testReentrantLock() {
        String currentLeaderNodeId = getCurrentLeaderNodeId();
        TestNode leaderNode = findNodeById(currentLeaderNodeId);
        LockStateMachine leaderStateMachine = findStateMachineByNodeId(currentLeaderNodeId);

        DistributedLock lock = new DistributedLockImpl(
            "reentrant-lock", currentLeaderNodeId, leaderNode.getRaftNode(), leaderStateMachine
        );

        try (LockHandle handle1 = lock.tryLock(2000)) {
            assertTrue(handle1.isSuccess());
            assertTrue(lock.isLocked());

            try (LockHandle handle2 = lock.tryLock(2000)) {
                assertTrue(handle2.isSuccess());
                assertTrue(lock.isLocked());
            }

            assertTrue(lock.isLocked(), "Lock should still be held after inner handle close");
        }

        assertFalse(lock.isLocked(), "Lock should be released after outer handle close");
    }

    @Test
    void testUnlockTwice() {
        String currentLeaderNodeId = getCurrentLeaderNodeId();
        TestNode leaderNode = findNodeById(currentLeaderNodeId);
        LockStateMachine leaderStateMachine = findStateMachineByNodeId(currentLeaderNodeId);

        DistributedLock lock = new DistributedLockImpl(
            "test-lock-3", currentLeaderNodeId, leaderNode.getRaftNode(), leaderStateMachine
        );

        LockHandle handle = lock.tryLock(2000);
        assertTrue(handle.isSuccess());

        handle.close();
        assertFalse(lock.isLocked());

        handle.close();
        assertFalse(lock.isLocked());
    }

    private TestNode findNodeById(String nodeId) {
        for (TestNode node : nodes) {
            if (node.getNodeId().equals(nodeId)) {
                return node;
            }
        }
        throw new IllegalArgumentException("Node not found: " + nodeId);
    }

    private LockStateMachine findStateMachineByNodeId(String nodeId) {
        for (int i = 0; i < nodes.size(); i++) {
            if (nodes.get(i).getNodeId().equals(nodeId)) {
                return stateMachines.get(i);
            }
        }
        throw new IllegalArgumentException("Node not found: " + nodeId);
    }

    @Test
    void testLockTransferAfterLeaderFailure() throws InterruptedException {
        String currentLeaderNodeId = getCurrentLeaderNodeId();
        TestNode oldLeaderNode = findNodeById(currentLeaderNodeId);
        LockStateMachine oldLeaderStateMachine = findStateMachineByNodeId(currentLeaderNodeId);
        logger.info("testLockTransferAfterLeaderFailure: currentLeaderNodeId={}", currentLeaderNodeId);
        logger.info("oldLeaderStateMachine lock table: {}", oldLeaderStateMachine.getLockCount());

        DistributedLock lock = new DistributedLockImpl(
            "transfer-lock", currentLeaderNodeId, oldLeaderNode.getRaftNode(), oldLeaderStateMachine
        );

        LockHandle handle = lock.tryLock(2000);
        logger.info("Lock tryLock result: success={}, locked={}, holder={}", 
            handle.isSuccess(), lock.isLocked(), lock.getHolderNodeId());
        assertTrue(handle.isSuccess(), "Leader应能获取锁");
        assertTrue(lock.isLocked(), "锁应被持有");
        assertEquals(currentLeaderNodeId, lock.getHolderNodeId(), "锁持有者应为原Leader");

        handle.close();
        assertFalse(lock.isLocked(), "锁应被释放");

        List<TestNode> otherNodes = new ArrayList<>();
        for (TestNode node : nodes) {
            if (!node.getNodeId().equals(currentLeaderNodeId)) {
                otherNodes.add(node);
            }
        }

        for (TestNode node : otherNodes) {
            node.disconnectFrom(currentLeaderNodeId);
        }

        oldLeaderNode.shutdown();

        CountDownLatch newLeaderElected = new CountDownLatch(1);
        Thread monitorThread = new Thread(() -> {
            while (newLeaderElected.getCount() > 0) {
                for (TestNode node : nodes) {
                    if (node.isLeader() && !node.getNodeId().equals(currentLeaderNodeId)) {
                        newLeaderElected.countDown();
                        return;
                    }
                }
                try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
            }
        });
        monitorThread.start();

        boolean newElected = newLeaderElected.await(5, TimeUnit.SECONDS);
        monitorThread.interrupt();
        monitorThread.join(100);

        assertTrue(newElected, "应在5秒内选出新Leader");

        TestNode newLeaderNode = null;
        String newLeaderId = null;
        LockStateMachine newLeaderStateMachine = null;
        for (TestNode node : nodes) {
            if (node.isLeader() && !node.getNodeId().equals(currentLeaderNodeId)) {
                newLeaderNode = node;
                newLeaderId = node.getNodeId();
                newLeaderStateMachine = findStateMachineByNodeId(newLeaderId);
                break;
            }
        }

        assertNotNull(newLeaderNode, "应有新Leader");
        assertNotNull(newLeaderId, "应有新Leader ID");
        assertNotNull(newLeaderStateMachine, "应有新Leader状态机");

        DistributedLock newLock = new DistributedLockImpl(
            "transfer-lock", newLeaderId, newLeaderNode.getRaftNode(), newLeaderStateMachine
        );

        LockHandle newHandle = newLock.tryLock(2000);
        assertTrue(newHandle.isSuccess(), "新Leader应能获取锁");
        assertTrue(newLock.isLocked(), "锁应被新Leader持有");
        assertEquals(newLeaderId, newLock.getHolderNodeId(), "锁持有者应为新Leader");

        handle.close();
        newLock.unlock();
    }

    @Test
    void testLockStateConsistencyAfterPartition() throws InterruptedException {
        String currentLeaderNodeId = getCurrentLeaderNodeId();
        List<String> allNodeIds = Arrays.asList("node1", "node2", "node3");

        for (String nodeId : allNodeIds) {
            if (!nodeId.equals(currentLeaderNodeId)) {
                TestNode node = findNodeById(nodeId);
                node.disconnectFrom(currentLeaderNodeId);
            }
        }

        Thread.sleep(2000);

        TestNode leaderNode = findNodeById(currentLeaderNodeId);
        LockStateMachine leaderStateMachine = findStateMachineByNodeId(currentLeaderNodeId);

        DistributedLock lock = new DistributedLockImpl(
            "partition-lock", currentLeaderNodeId, leaderNode.getRaftNode(), leaderStateMachine
        );

        LockHandle handle = lock.tryLock(2000);
        assertTrue(handle.isSuccess(), "Leader应在分区后仍能获取锁");

        // raft 单线程化后 commit/apply 变为异步推进，多数派一致性按"最终一致"轮询验收
        boolean consistent = false;
        long deadline = System.currentTimeMillis() + 1000;
        while (System.currentTimeMillis() < deadline && !consistent) {
            consistent = leaderStateMachine.isLockHeldBy("partition-lock", currentLeaderNodeId);
            if (!consistent) {
                Thread.sleep(10);
            }
        }
        assertTrue(consistent, "锁状态应在多数派节点保持一致");

        handle.close();
    }
}