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
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DistributedLockIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(DistributedLockIntegrationTest.class);

    private List<TestNode> nodes;
    private List<LockStateMachine> stateMachines;
    private String leaderNodeId;

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

        leaderNodeId = null;
        for (TestNode node : nodes) {
            if (node.isLeader()) {
                leaderNodeId = node.getNodeId();
                logger.info("Leader elected: {}", leaderNodeId);
                break;
            }
        }

        assertNotNull(leaderNodeId, "Leader should be elected within 500ms");
    }

    @AfterEach
    void tearDown() {
        for (TestNode node : nodes) {
            node.shutdown();
        }
    }

    @Test
    void testLeaderCanAcquireLock() {
        TestNode leaderNode = findNodeById(leaderNodeId);
        LockStateMachine leaderStateMachine = findStateMachineByNodeId(leaderNodeId);

        DistributedLock lock = new DistributedLockImpl(
            "test-lock", leaderNodeId, leaderNode.getRaftNode(), leaderStateMachine
        );

        LockHandle handle = lock.tryLock(2000);

        assertTrue(handle.isSuccess(), "Leader should be able to acquire lock");
        assertEquals("test-lock", handle.getLockName());
        assertEquals(leaderNodeId, handle.getNodeId());

        assertTrue(lock.isLocked());
        assertTrue(lock.isHeldByCurrentNode());
        assertEquals(leaderNodeId, lock.getHolderNodeId());

        handle.close();

        assertFalse(lock.isLocked());
    }

    @Test
    void testTryWithResources() {
        TestNode leaderNode = findNodeById(leaderNodeId);
        LockStateMachine leaderStateMachine = findStateMachineByNodeId(leaderNodeId);

        DistributedLock lock = new DistributedLockImpl(
            "test-lock-2", leaderNodeId, leaderNode.getRaftNode(), leaderStateMachine
        );

        try (LockHandle handle = lock.tryLock(2000)) {
            assertTrue(handle.isSuccess());
            assertTrue(lock.isLocked());
        }

        assertFalse(lock.isLocked());
    }

    @Test
    void testMultipleLocks() {
        TestNode leaderNode = findNodeById(leaderNodeId);
        LockStateMachine leaderStateMachine = findStateMachineByNodeId(leaderNodeId);

        DistributedLock lock1 = new DistributedLockImpl(
            "lock-1", leaderNodeId, leaderNode.getRaftNode(), leaderStateMachine
        );
        DistributedLock lock2 = new DistributedLockImpl(
            "lock-2", leaderNodeId, leaderNode.getRaftNode(), leaderStateMachine
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
        TestNode leaderNode = findNodeById(leaderNodeId);
        LockStateMachine leaderStateMachine = findStateMachineByNodeId(leaderNodeId);

        DistributedLock lock = new DistributedLockImpl(
            "reentrant-lock", leaderNodeId, leaderNode.getRaftNode(), leaderStateMachine
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
        TestNode leaderNode = findNodeById(leaderNodeId);
        LockStateMachine leaderStateMachine = findStateMachineByNodeId(leaderNodeId);

        DistributedLock lock = new DistributedLockImpl(
            "test-lock-3", leaderNodeId, leaderNode.getRaftNode(), leaderStateMachine
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
}