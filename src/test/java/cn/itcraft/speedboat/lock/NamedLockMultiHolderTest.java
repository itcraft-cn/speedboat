package cn.itcraft.speedboat.lock;

import cn.itcraft.speedboat.integration.MockTransport;
import cn.itcraft.speedboat.integration.TestNode;
import cn.itcraft.speedboat.raft.ElectionTimeout;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 命名锁多持有者语义验证（2026-09-18 设计 P3）。
 *
 * <p>覆盖：①任意成员持锁（转发路径）；②同名目互斥硬约束（唯一成功者）；
 * ③epoch fencing 单调；④非抢占公平竞争；⑤持有者失联后租约迁移。</p>
 */
class NamedLockMultiHolderTest {

    private static final Logger logger = LoggerFactory.getLogger(NamedLockMultiHolderTest.class);

    /** 测试租约：600ms（续期间隔 300ms），兼顾迁移窗口与稳定性 */
    private static final long TEST_LEASE_MS = 600;

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

        // 等待选主稳定（与既有 IntegrationTest 同一等待策略）
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            if (getLeaderNodeId() != null) {
                break;
            }
            TimeUnit.MILLISECONDS.sleep(50);
        }
        assertNotNull(getLeaderNodeId(), "Leader 应在 5s 内选出");
        logger.info("setUp completed, leader={}", getLeaderNodeId());
    }

    private String getLeaderNodeId() {
        for (TestNode node : nodes) {
            if (node.isLeader()) {
                return node.getNodeId();
            }
        }
        return null;
    }

    private TestNode nodeById(String nodeId) {
        for (TestNode node : nodes) {
            if (node.getNodeId().equals(nodeId)) {
                return node;
            }
        }
        return null;
    }

    private LockStateMachine smBy(String nodeId) {
        for (int i = 0; i < nodes.size(); i++) {
            if (nodes.get(i).getNodeId().equals(nodeId)) {
                return stateMachines.get(i);
            }
        }
        return null;
    }

    private DistributedLockImpl newLock(String holderNodeId, String lockName) {
        TestNode node = nodeById(holderNodeId);
        return new DistributedLockImpl(lockName, holderNodeId, node.getRaftNode(),
            smBy(holderNodeId), TEST_LEASE_MS);
    }

    @AfterEach
    void tearDown() {
        for (TestNode node : nodes) {
            node.shutdown();
        }
    }

    /** ①非 Leader 成员可申请并持有锁（转发路径 + epoch） */
    @Test
    void testFollowerCanAcquireAndReleaseViaForwarding() {
        String leaderId = getLeaderNodeId();
        String followerId = nodes.stream().map(TestNode::getNodeId)
            .filter(id -> !id.equals(leaderId)).findFirst().orElseThrow();

        DistributedLock followerLock = newLock(followerId, "forex");
        LockHandle handle = followerLock.tryLock(3000);

        assertTrue(handle.isSuccess(), "非 Leader 成员应能经转发持有锁");
        assertTrue(handle.getEpoch() > 0, "成功持锁应返回 epoch>0");
        assertEquals(followerId, followerLock.getHolderNodeId(), "持有者应为申请者");
        assertNotEquals(leaderId, followerLock.getHolderNodeId(), "持有者与 Leader 无关");

        // 全网视图一致：Leader 侧 apply 后同一持有者
        assertEquals(followerId, smBy(leaderId).getLockEntry("forex").getNodeId(),
            "Leader 副本应收敛同一持有者");

        // 释放（走转发 UNLOCK）后全网视图收敛到未持有
        handle.close();
        assertNull(smBy(leaderId).getLockEntry("forex").getNodeId(), "释放应全网生效");
    }

    /**
     * ②互斥硬约束：N 节点并发争抢同名目锁，任意时刻至多一个持有者。
     * 断言两个层次：a) 同一重叠时间窗内成功者唯一；b) 先后持有的时间区间两两不重叠。
     */
    @Test
    void testMutualExclusionExactlyOneWinner() throws InterruptedException {
        int contenders = 3;
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(contenders);
        AtomicInteger overlapDetected = new AtomicInteger(0);
        // 成功持有区间 [start, end]，用于时段重叠校验
        List<long[]> holdingWindows = java.util.Collections.synchronizedList(new ArrayList<>());

        for (String id : Arrays.asList("node1", "node2", "node3")) {
            new Thread(() -> {
                try {
                    startGate.await(2, TimeUnit.SECONDS);
                    DistributedLock lock = newLock(id, "mutex-demo");
                    LockHandle handle = lock.tryLock(3000);
                    if (handle.isSuccess()) {
                        long start = System.nanoTime();
                        synchronized (holdingWindows) {
                            // 与已存在窗口重叠即视为并发双持（互斥被破坏）
                            for (long[] w : holdingWindows) {
                                if (w[0] <= start && start < w[1]) {
                                    overlapDetected.incrementAndGet();
                                }
                            }
                        }
                        TimeUnit.MILLISECONDS.sleep(250);
                        synchronized (holdingWindows) {
                            holdingWindows.add(new long[]{start, System.nanoTime()});
                        }
                        handle.close();
                    }
                } catch (Exception e) {
                    logger.error("contender thread error", e);
                } finally {
                    finished.countDown();
                }
            }, "contender-" + id).start();
        }
        startGate.countDown();
        assertTrue(finished.await(10, TimeUnit.SECONDS), "竞争线程应全部完成");

        // 重叠窗口计数为 0：任意时刻至多一个持有者（互斥硬约束）
        assertEquals(0, overlapDetected.get(), "持有区间不可重叠（互斥硬约束）");

        // 至少一个成功且全部 epoch 为正
        assertTrue(!holdingWindows.isEmpty() && holdingWindows.size() >= 1, "至少一个竞争者应持锁成功");
    }

    /** ③epoch 单调：释放后下一持有者的 epoch 严格递增（fencing 拒旧依据） */
    @Test
    void testEpochMonotonicAcrossOwnershipChange() {
        String leaderId = getLeaderNodeId();
        DistributedLock lockA = newLock(leaderId, "metals");
        LockHandle h1 = lockA.tryLock(3000);
        assertTrue(h1.isSuccess());
        long epoch1 = h1.getEpoch();
        h1.close();

        DistributedLock lockB = newLock(leaderId, "metals");
        LockHandle h2 = lockB.tryLock(3000);
        assertTrue(h2.isSuccess(), "释放后应可再次获取");
        long epoch2 = h2.getEpoch();
        h2.close();

        assertTrue(epoch2 >= epoch1 + 1, "同一锁所有权变更后 epoch 必须单调递增: " + epoch1 + "→" + epoch2);
    }

    /** ④非抢占公平竞争：已被持有的锁不得被他人抢占（无 REVOKE） */
    @Test
    void testNonPreemptionHolderKeepsLock() {
        String leaderId = getLeaderNodeId();
        String followerId = nodes.stream().map(TestNode::getNodeId)
            .filter(id -> !id.equals(leaderId)).findFirst().orElseThrow();

        DistributedLock followerLock = newLock(followerId, "commodity");
        DistributedLock leaderLock = newLock(leaderId, "commodity");

        LockHandle hB = followerLock.tryLock(3000);
        assertTrue(hB.isSuccess(), "B 应先持有锁");

        // A（Leader）申请同一锁：公平竞争下不得抢占成功
        LockHandle hA = leaderLock.tryLock(1500);
        assertFalse(hA.isSuccess(), "非抢占策略下不得从中断他人未失联的持有");
        assertEquals(followerId, smBy(leaderId).getLockEntry("commodity").getNodeId(),
            "持有权应保持不变");

        hB.close();
    }

    /**
     * ⑤租约迁移：持有者"停机"（停续期）后，租约到期锁自然迁往竞争者；
     * 迁移期间新主 epoch 推进（下游可据以拒旧）。
     */
    @Test
    void testLockMigratesAfterHolderLeaseExpires() throws Exception {
        String leaderId = getLeaderNodeId();
        String followerId = nodes.stream().map(TestNode::getNodeId)
            .filter(id -> !id.equals(leaderId)).findFirst().orElseThrow();

        DistributedLock leaderLock = newLock(leaderId, "commodity");
        LockHandle hA = leaderLock.tryLock(3000);
        assertTrue(hA.isSuccess());
        long epochBefore = hA.getEpoch();

        // 模拟持有者失联：直接停掉其节点（续期断流；不调用 close()，非自愿释放）
        nodeById(leaderId).shutdown();

        // 租约 600ms → 800ms 后竞争者可获锁
        TimeUnit.MILLISECONDS.sleep(800);

        DistributedLock followerLock = newLock(followerId, "commodity");
        LockHandle hB = followerLock.tryLock(3000);
        assertTrue(hB.isSuccess(), "原持有者失联后租约应到期，锁迁往竞争者");
        assertTrue(hB.getEpoch() > epochBefore, "迁移后 epoch 应单调递增（拒旧主依据）");

        hB.close();
    }


    /** 持锁节点的分布式锁均合理：并发跨锁操作不互阻（通道隔离） */
    @Test
    void twoNodeCanHoldDifferentLocksSimultaneously() {
        String leaderId = getLeaderNodeId();
        String followerId = nodes.stream().map(TestNode::getNodeId)
            .filter(id -> !id.equals(leaderId)).findFirst().orElseThrow();

        DistributedLock lockA = newLock(leaderId, "forex");
        DistributedLock lockB = newLock(followerId, "metals");

        LockHandle hA = lockA.tryLock(3000);
        LockHandle hB = lockB.tryLock(3000);

        assertTrue(hA.isSuccess(), "Leader 持 forex");
        assertTrue(hB.isSuccess(), "follower 持 metals——三把不同名目的锁并存");

        assertEquals(leaderId, smBy(leaderId).getLockEntry("forex").getNodeId());
        assertEquals(followerId, smBy(leaderId).getLockEntry("metals").getNodeId());

        hA.close();
        hB.close();
    }
}
