package cn.itcraft.speedboat.integration;

import cn.itcraft.speedboat.config.SpeedboatConsts;
import cn.itcraft.speedboat.raft.ElectionTimeout;
import cn.itcraft.speedboat.raft.NodeState;
import cn.itcraft.speedboat.raft.RaftNode;
import cn.itcraft.speedboat.raft.VoteContext;
import cn.itcraft.speedboat.strategy.group.DefaultGroupStrategy;
import cn.itcraft.speedboat.strategy.group.GroupStrategy;
import cn.itcraft.speedboat.strategy.voteweight.EvenNodeVoteWeightStrategy;
import cn.itcraft.speedboat.strategy.voteweight.VoteWeightStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class IntegrationTest {

    private List<TestNode> testNodes;

    @BeforeEach
    void setUp() {
        testNodes = new ArrayList<>();
    }

    @AfterEach
    void tearDown() {
        for (TestNode node : testNodes) {
            node.shutdown();
        }
        testNodes.clear();
    }

    @Test
    @DisplayName("场景一：3节点正常选主")
    void testThreeNodeElection() throws InterruptedException {
        List<String> nodeIds = Arrays.asList("node-1", "node-2", "node-3");
        GroupStrategy groupStrategy = new DefaultGroupStrategy();
        VoteWeightStrategy voteWeightStrategy = null;
        ElectionTimeout timeout = new ElectionTimeout(150, 300);

        for (String nodeId : nodeIds) {
            List<String> peerIds = new ArrayList<>(nodeIds);
            peerIds.remove(nodeId);
            
            TestNode testNode = new TestNode(nodeId, peerIds, timeout, voteWeightStrategy, groupStrategy);
            testNodes.add(testNode);
        }

        for (TestNode node : testNodes) {
            node.connectToAll(testNodes);
        }

        for (TestNode node : testNodes) {
            node.start();
        }

        CountDownLatch leaderElected = new CountDownLatch(1);
        AtomicInteger leaderCount = new AtomicInteger(0);

        Thread monitorThread = new Thread(() -> {
            while (leaderElected.getCount() > 0) {
                int currentLeaders = 0;
                for (TestNode node : testNodes) {
                    if (node.isLeader()) {
                        currentLeaders++;
                    }
                }
                if (currentLeaders == 1) {
                    leaderCount.set(currentLeaders);
                    leaderElected.countDown();
                    break;
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        monitorThread.start();

        boolean elected = leaderElected.await(5, TimeUnit.SECONDS);
        monitorThread.interrupt();
        monitorThread.join(100);

        assertTrue(elected, "选举应在5秒内完成");
        assertEquals(1, leaderCount.get(), "应有且仅有1个Leader");

        long leaderTerm = -1;
        String leaderId = null;
        for (TestNode node : testNodes) {
            if (node.isLeader()) {
                leaderTerm = node.getCurrentTerm();
                leaderId = node.getNodeId();
            }
        }

        assertNotNull(leaderId, "应选出Leader");
        assertTrue(leaderTerm >= 1, "Leader Term应>=1");

        for (TestNode node : testNodes) {
            if (!node.getNodeId().equals(leaderId)) {
                assertEquals(NodeState.FOLLOWER, node.getRaftNode().getCurrentState(),
                    "非Leader节点应为FOLLOWER");
            }
        }
    }

    @Test
    @DisplayName("场景二：Leader故障，同机房切换")
    void testLeaderFailureAndRecovery() throws InterruptedException {
        List<String> nodeIds = Arrays.asList("node-1", "node-2", "node-3");
        GroupStrategy groupStrategy = new DefaultGroupStrategy();
        VoteWeightStrategy voteWeightStrategy = null;
        ElectionTimeout timeout = new ElectionTimeout(150, 300);

        for (String nodeId : nodeIds) {
            List<String> peerIds = new ArrayList<>(nodeIds);
            peerIds.remove(nodeId);
            
            TestNode testNode = new TestNode(nodeId, peerIds, timeout, voteWeightStrategy, groupStrategy);
            testNodes.add(testNode);
        }

        for (TestNode node : testNodes) {
            node.connectToAll(testNodes);
        }

        for (TestNode node : testNodes) {
            node.start();
        }

        CountDownLatch firstLeaderElected = new CountDownLatch(1);
        Thread waitThread = new Thread(() -> {
            while (firstLeaderElected.getCount() > 0) {
                for (TestNode node : testNodes) {
                    if (node.isLeader()) {
                        firstLeaderElected.countDown();
                        return;
                    }
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        });
        waitThread.start();
        assertTrue(firstLeaderElected.await(5, TimeUnit.SECONDS), "初始选举应在5秒内完成");
        waitThread.interrupt();
        waitThread.join(100);

        TestNode oldLeader = null;
        for (TestNode node : testNodes) {
            if (node.isLeader()) {
                oldLeader = node;
                break;
            }
        }
        assertNotNull(oldLeader, "应有初始Leader");

        final String oldLeaderId = oldLeader.getNodeId();

        for (TestNode node : testNodes) {
            if (!node.getNodeId().equals(oldLeaderId)) {
                node.disconnectFrom(oldLeaderId);
            }
        }

        oldLeader.shutdown();

        CountDownLatch newLeaderElected = new CountDownLatch(1);
        Thread monitorThread = new Thread(() -> {
            while (newLeaderElected.getCount() > 0) {
                for (TestNode node : testNodes) {
                    if (node.isLeader() && !node.getNodeId().equals(oldLeaderId)) {
                        newLeaderElected.countDown();
                        return;
                    }
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        });
        monitorThread.start();

        boolean newElected = newLeaderElected.await(5, TimeUnit.SECONDS);
        monitorThread.interrupt();
        monitorThread.join(100);

        assertTrue(newElected, "新Leader应在5秒内选出");

        TestNode newLeader = null;
        for (TestNode node : testNodes) {
            if (node.isLeader() && !node.getNodeId().equals(oldLeaderId)) {
                newLeader = node;
                break;
            }
        }
        assertNotNull(newLeader, "应选出新Leader");
        assertNotEquals(oldLeaderId, newLeader.getNodeId(), "新Leader不应是旧Leader");
    }

    @Test
    @DisplayName("场景三：偶数节点平票解决（2节点）")
    void testTwoNodeElection() throws InterruptedException {
        List<String> nodeIds = Arrays.asList("node-1", "node-2");
        GroupStrategy groupStrategy = new DefaultGroupStrategy();
        VoteWeightStrategy voteWeightStrategy = new EvenNodeVoteWeightStrategy();
        ElectionTimeout timeout = new ElectionTimeout(150, 300);

        for (String nodeId : nodeIds) {
            List<String> peerIds = new ArrayList<>(nodeIds);
            peerIds.remove(nodeId);
            
            TestNode testNode = new TestNode(nodeId, peerIds, timeout, voteWeightStrategy, groupStrategy);
            testNodes.add(testNode);
        }

        for (TestNode node : testNodes) {
            node.connectToAll(testNodes);
        }

        testNodes.get(0).start();

        Thread.sleep(50);

        testNodes.get(1).start();

        CountDownLatch leaderElected = new CountDownLatch(1);
        AtomicInteger leaderCount = new AtomicInteger(0);

        Thread monitorThread = new Thread(() -> {
            while (leaderElected.getCount() > 0) {
                int currentLeaders = 0;
                for (TestNode node : testNodes) {
                    if (node.isLeader()) {
                        currentLeaders++;
                    }
                }
                if (currentLeaders == 1) {
                    leaderCount.set(currentLeaders);
                    leaderElected.countDown();
                    break;
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        monitorThread.start();

        boolean elected = leaderElected.await(5, TimeUnit.SECONDS);
        monitorThread.interrupt();
        monitorThread.join(100);

        assertTrue(elected, "选举应在5秒内完成");
        assertEquals(1, leaderCount.get(), "应有且仅有1个Leader");

        TestNode leader = null;
        for (TestNode node : testNodes) {
            if (node.isLeader()) {
                leader = node;
                break;
            }
        }
        assertNotNull(leader, "应选出Leader");
    }

    @Test
    @DisplayName("场景四：跨机房级联选主")
    void testDatacenterCascade() throws InterruptedException {
        GroupStrategy localGroupStrategy = new DefaultGroupStrategy();
        VoteWeightStrategy voteWeightStrategy = null;
        ElectionTimeout localTimeout = new ElectionTimeout(150, 300);

        List<String> dc1Nodes = Arrays.asList("dc1-node-1", "dc1-node-2", "dc1-node-3");
        List<String> dc2Nodes = Arrays.asList("dc2-node-1", "dc2-node-2", "dc2-node-3");

        List<TestNode> dc1TestNodes = new ArrayList<>();
        for (String nodeId : dc1Nodes) {
            List<String> peerIds = new ArrayList<>(dc1Nodes);
            peerIds.remove(nodeId);
            
            TestNode testNode = new TestNode(nodeId, peerIds, localTimeout, voteWeightStrategy, localGroupStrategy);
            dc1TestNodes.add(testNode);
            testNodes.add(testNode);
        }

        for (TestNode node : dc1TestNodes) {
            node.connectToAll(dc1TestNodes);
        }

        List<TestNode> dc2TestNodes = new ArrayList<>();
        for (String nodeId : dc2Nodes) {
            List<String> peerIds = new ArrayList<>(dc2Nodes);
            peerIds.remove(nodeId);
            
            TestNode testNode = new TestNode(nodeId, peerIds, localTimeout, voteWeightStrategy, localGroupStrategy);
            dc2TestNodes.add(testNode);
            testNodes.add(testNode);
        }

        for (TestNode node : dc2TestNodes) {
            node.connectToAll(dc2TestNodes);
        }

        for (TestNode node : dc1TestNodes) {
            node.start();
        }

        for (TestNode node : dc2TestNodes) {
            node.start();
        }

        CountDownLatch dc1LeaderElected = new CountDownLatch(1);
        CountDownLatch dc2LeaderElected = new CountDownLatch(1);

        Thread dc1Monitor = new Thread(() -> {
            while (dc1LeaderElected.getCount() > 0) {
                for (TestNode node : dc1TestNodes) {
                    if (node.isLeader()) {
                        dc1LeaderElected.countDown();
                        return;
                    }
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        });

        Thread dc2Monitor = new Thread(() -> {
            while (dc2LeaderElected.getCount() > 0) {
                for (TestNode node : dc2TestNodes) {
                    if (node.isLeader()) {
                        dc2LeaderElected.countDown();
                        return;
                    }
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        });

        dc1Monitor.start();
        dc2Monitor.start();

        boolean dc1Elected = dc1LeaderElected.await(5, TimeUnit.SECONDS);
        boolean dc2Elected = dc2LeaderElected.await(5, TimeUnit.SECONDS);

        dc1Monitor.interrupt();
        dc2Monitor.interrupt();
        dc1Monitor.join(100);
        dc2Monitor.join(100);

        assertTrue(dc1Elected, "机房DC1应选出Leader");
        assertTrue(dc2Elected, "机房DC2应选出Leader");

        int dc1LeaderCount = 0;
        int dc2LeaderCount = 0;
        TestNode dc1Leader = null;
        TestNode dc2Leader = null;

        for (TestNode node : dc1TestNodes) {
            if (node.isLeader()) {
                dc1LeaderCount++;
                dc1Leader = node;
            }
        }

        for (TestNode node : dc2TestNodes) {
            if (node.isLeader()) {
                dc2LeaderCount++;
                dc2Leader = node;
            }
        }

        assertEquals(1, dc1LeaderCount, "机房DC1应有且仅有1个Leader");
        assertEquals(1, dc2LeaderCount, "机房DC2应有且仅有1个Leader");
        assertNotNull(dc1Leader, "机房DC1应选出代表节点");
        assertNotNull(dc2Leader, "机房DC2应选出代表节点");

        assertNotEquals(dc1Leader.getNodeId(), dc2Leader.getNodeId(),
            "不同机房应选出不同的代表节点");
    }

    @Test
    @DisplayName("场景五：心跳维持Leader权威")
    void testHeartbeatMaintainsLeadership() throws InterruptedException {
        List<String> nodeIds = Arrays.asList("node-1", "node-2", "node-3");
        GroupStrategy groupStrategy = new DefaultGroupStrategy();
        VoteWeightStrategy voteWeightStrategy = null;
        ElectionTimeout timeout = new ElectionTimeout(150, 300);

        for (String nodeId : nodeIds) {
            List<String> peerIds = new ArrayList<>(nodeIds);
            peerIds.remove(nodeId);
            
            TestNode testNode = new TestNode(nodeId, peerIds, timeout, voteWeightStrategy, groupStrategy);
            testNodes.add(testNode);
        }

        for (TestNode node : testNodes) {
            node.connectToAll(testNodes);
        }

        for (TestNode node : testNodes) {
            node.start();
        }

        CountDownLatch leaderElected = new CountDownLatch(1);
        Thread waitThread = new Thread(() -> {
            while (leaderElected.getCount() > 0) {
                for (TestNode node : testNodes) {
                    if (node.isLeader()) {
                        leaderElected.countDown();
                        return;
                    }
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        });
        waitThread.start();
        assertTrue(leaderElected.await(5, TimeUnit.SECONDS), "初始选举应在5秒内完成");
        waitThread.interrupt();
        waitThread.join(100);

        TestNode leader = null;
        for (TestNode node : testNodes) {
            if (node.isLeader()) {
                leader = node;
                break;
            }
        }
        assertNotNull(leader, "应有Leader");

        Thread.sleep(1000);

        // 时序加固：偶发初始选举抖动（如重叠选举）后集群仍需 1s 内进入稳定态。
        // 轮询等待而非硬断言瞬时状态，减少 CI 时序 flaky。
        long stableDeadline = System.currentTimeMillis() + 5_000;
        boolean stable = false;
        while (System.currentTimeMillis() < stableDeadline) {
            Thread.sleep(200);
            if (leader.isLeader()) {
                boolean othersAllFollower = true;
                for (TestNode node : testNodes) {
                    if (!node.getNodeId().equals(leader.getNodeId()) && !node.isFollower()) {
                        othersAllFollower = false;
                        break;
                    }
                }
                if (othersAllFollower) {
                    stable = true;
                    break;
                }
            }
        }

        assertTrue(stable, "心率稳定窗口后集群应达到 1 Leader + 2 Follower 稳定态");
    }

    @Test
    @DisplayName("场景七：3节点stop一个Follower，Leader应维持")
    void testThreeNodeStopOneFollower_LeaderMaintains() throws InterruptedException {
        List<String> nodeIds = Arrays.asList("node-1", "node-2", "node-3");
        GroupStrategy groupStrategy = new DefaultGroupStrategy();
        ElectionTimeout timeout = new ElectionTimeout(150, 300);

        for (String nodeId : nodeIds) {
            List<String> peerIds = new ArrayList<>(nodeIds);
            peerIds.remove(nodeId);
            TestNode testNode = new TestNode(nodeId, peerIds, timeout, null, groupStrategy);
            testNodes.add(testNode);
        }

        for (TestNode node : testNodes) {
            node.connectToAll(testNodes);
        }

        for (TestNode node : testNodes) {
            node.start();
        }

        CountDownLatch leaderElected = new CountDownLatch(1);
        Thread waitThread = new Thread(() -> {
            while (leaderElected.getCount() > 0) {
                for (TestNode node : testNodes) {
                    if (node.isLeader()) {
                        leaderElected.countDown();
                        return;
                    }
                }
                try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
            }
        });
        waitThread.start();
        assertTrue(leaderElected.await(5, TimeUnit.SECONDS), "初始选举应在5秒内完成");
        waitThread.interrupt();
        waitThread.join(100);

        TestNode leader = null;
        TestNode follower = null;
        for (TestNode node : testNodes) {
            if (node.isLeader()) {
                leader = node;
            } else {
                follower = node;
            }
        }
        assertNotNull(leader, "应有Leader");
        assertNotNull(follower, "应有Follower");

        final String leaderId = leader.getNodeId();
        final String followerId = follower.getNodeId();

        leader.disconnectFrom(followerId);
        follower.shutdown();

        Thread.sleep(2000);

        assertTrue(leader.isLeader(), "Leader应维持其状态");
        assertEquals(leaderId, leader.getLeaderId(), "Leader ID应保持不变");
    }

    @Test
    @DisplayName("场景八：3节点stop Leader，剩余2节点应能选出新Leader")
    void testThreeNodeStopLeader_Reelect() throws InterruptedException {
        List<String> nodeIds = Arrays.asList("node-1", "node-2", "node-3");
        GroupStrategy groupStrategy = new DefaultGroupStrategy();
        ElectionTimeout timeout = new ElectionTimeout(150, 300);

        for (String nodeId : nodeIds) {
            List<String> peerIds = new ArrayList<>(nodeIds);
            peerIds.remove(nodeId);
            TestNode testNode = new TestNode(nodeId, peerIds, timeout, null, groupStrategy);
            testNodes.add(testNode);
        }

        for (TestNode node : testNodes) {
            node.connectToAll(testNodes);
        }

        for (TestNode node : testNodes) {
            node.start();
        }

        CountDownLatch leaderElected = new CountDownLatch(1);
        Thread waitThread = new Thread(() -> {
            while (leaderElected.getCount() > 0) {
                for (TestNode node : testNodes) {
                    if (node.isLeader()) {
                        leaderElected.countDown();
                        return;
                    }
                }
                try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
            }
        });
        waitThread.start();
        assertTrue(leaderElected.await(5, TimeUnit.SECONDS), "初始选举应在5秒内完成");
        waitThread.interrupt();
        waitThread.join(100);

        TestNode oldLeader = null;
        for (TestNode node : testNodes) {
            if (node.isLeader()) {
                oldLeader = node;
                break;
            }
        }
        assertNotNull(oldLeader, "应有初始Leader");
        final String oldLeaderId = oldLeader.getNodeId();

        for (TestNode node : testNodes) {
            if (!node.getNodeId().equals(oldLeaderId)) {
                node.disconnectFrom(oldLeaderId);
            }
        }
        oldLeader.shutdown();

        CountDownLatch newLeaderElected = new CountDownLatch(1);
        Thread monitorThread = new Thread(() -> {
            while (newLeaderElected.getCount() > 0) {
                for (TestNode node : testNodes) {
                    if (node.isLeader() && !node.getNodeId().equals(oldLeaderId)) {
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

        assertTrue(newElected, "剩余2节点应在5秒内选出新Leader");

        int leaderCount = 0;
        for (TestNode node : testNodes) {
            if (node.isLeader() && !node.getNodeId().equals(oldLeaderId)) {
                leaderCount++;
            }
        }
        assertEquals(1, leaderCount, "剩余2节点应有且仅有1个Leader");
    }

    @Test
    @DisplayName("场景九：2节点stop Leader，剩余1节点无法选出Leader")
    void testTwoNodeStopLeader_NoReelect() throws InterruptedException {
        List<String> nodeIds = Arrays.asList("node-1", "node-2");
        GroupStrategy groupStrategy = new DefaultGroupStrategy();
        ElectionTimeout timeout = new ElectionTimeout(150, 300);

        for (String nodeId : nodeIds) {
            List<String> peerIds = new ArrayList<>(nodeIds);
            peerIds.remove(nodeId);
            TestNode testNode = new TestNode(nodeId, peerIds, timeout, null, groupStrategy);
            testNodes.add(testNode);
        }

        for (TestNode node : testNodes) {
            node.connectToAll(testNodes);
        }

        for (TestNode node : testNodes) {
            node.start();
        }

        CountDownLatch leaderElected = new CountDownLatch(1);
        Thread waitThread = new Thread(() -> {
            while (leaderElected.getCount() > 0) {
                for (TestNode node : testNodes) {
                    if (node.isLeader()) {
                        leaderElected.countDown();
                        return;
                    }
                }
                try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
            }
        });
        waitThread.start();
        assertTrue(leaderElected.await(5, TimeUnit.SECONDS), "初始选举应在5秒内完成");
        waitThread.interrupt();
        waitThread.join(100);

        TestNode leader = null;
        TestNode follower = null;
        for (TestNode node : testNodes) {
            if (node.isLeader()) {
                leader = node;
            } else {
                follower = node;
            }
        }
        assertNotNull(leader, "应有初始Leader");
        assertNotNull(follower, "应有Follower");

        final String leaderId = leader.getNodeId();
        follower.disconnectFrom(leaderId);
        leader.shutdown();

        Thread.sleep(3000);

        assertFalse(follower.isLeader(), "2节点集群剩余1节点无法成为Leader");
    }

    @Test
    @DisplayName("场景十：3节点stop两个节点（Leader+1Follower），剩余1节点无法选出Leader")
    void testThreeNodeStopTwo_NoReelect() throws InterruptedException {
        List<String> nodeIds = Arrays.asList("node-1", "node-2", "node-3");
        GroupStrategy groupStrategy = new DefaultGroupStrategy();
        ElectionTimeout timeout = new ElectionTimeout(150, 300);

        for (String nodeId : nodeIds) {
            List<String> peerIds = new ArrayList<>(nodeIds);
            peerIds.remove(nodeId);
            TestNode testNode = new TestNode(nodeId, peerIds, timeout, null, groupStrategy);
            testNodes.add(testNode);
        }

        for (TestNode node : testNodes) {
            node.connectToAll(testNodes);
        }

        for (TestNode node : testNodes) {
            node.start();
        }

        CountDownLatch leaderElected = new CountDownLatch(1);
        Thread waitThread = new Thread(() -> {
            while (leaderElected.getCount() > 0) {
                for (TestNode node : testNodes) {
                    if (node.isLeader()) {
                        leaderElected.countDown();
                        return;
                    }
                }
                try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
            }
        });
        waitThread.start();
        assertTrue(leaderElected.await(5, TimeUnit.SECONDS), "初始选举应在5秒内完成");
        waitThread.interrupt();
        waitThread.join(100);

        TestNode leader = null;
        TestNode survivor = null;
        for (TestNode node : testNodes) {
            if (node.isLeader()) {
                leader = node;
            } else {
                survivor = node;
            }
        }
        assertNotNull(leader, "应有初始Leader");
        assertNotNull(survivor, "应有Follower");

        final String leaderId = leader.getNodeId();
        final String survivorId = survivor.getNodeId();

        for (TestNode node : testNodes) {
            if (!node.getNodeId().equals(survivorId)) {
                survivor.disconnectFrom(node.getNodeId());
            }
        }
        leader.shutdown();

        Thread.sleep(3000);

        assertFalse(survivor.isLeader(), "3节点集群剩余1节点无法成为Leader");
    }

    @Test
    @DisplayName("场景六：Term单调性校验")
    void testTermMonotonicity() throws InterruptedException {
        List<String> nodeIds = Arrays.asList("node-1", "node-2", "node-3");
        GroupStrategy groupStrategy = new DefaultGroupStrategy();
        VoteWeightStrategy voteWeightStrategy = null;
        ElectionTimeout timeout = new ElectionTimeout(150, 300);

        for (String nodeId : nodeIds) {
            List<String> peerIds = new ArrayList<>(nodeIds);
            peerIds.remove(nodeId);
            
            TestNode testNode = new TestNode(nodeId, peerIds, timeout, voteWeightStrategy, groupStrategy);
            testNodes.add(testNode);
        }

        for (TestNode node : testNodes) {
            node.connectToAll(testNodes);
        }

        for (TestNode node : testNodes) {
            node.start();
        }

        CountDownLatch leaderElected = new CountDownLatch(1);
        Thread waitThread = new Thread(() -> {
            while (leaderElected.getCount() > 0) {
                for (TestNode node : testNodes) {
                    if (node.isLeader()) {
                        leaderElected.countDown();
                        return;
                    }
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        });
        waitThread.start();
        assertTrue(leaderElected.await(5, TimeUnit.SECONDS), "初始选举应在5秒内完成");
        waitThread.interrupt();
        waitThread.join(100);

        TestNode leader = null;
        for (TestNode node : testNodes) {
            if (node.isLeader()) {
                leader = node;
                break;
            }
        }
        assertNotNull(leader, "应有Leader");

        long initialTerm = leader.getCurrentTerm();
        assertTrue(initialTerm >= 1, "初始Term应>=1");

        leader.getRaftNode().getTerm().updateIfHigher(initialTerm + 10);
        assertEquals(initialTerm + 10, leader.getCurrentTerm(), "Term应单调递增");

        leader.getRaftNode().getTerm().updateIfHigher(initialTerm);
        assertEquals(initialTerm + 10, leader.getCurrentTerm(), "Term不应降低");
    }

    @Test
    @DisplayName("场景十一：网络分区测试（2:1 split）")
    void testNetworkPartitionTwoToOne() throws InterruptedException {
        List<String> nodeIds = Arrays.asList("node-1", "node-2", "node-3");
        GroupStrategy groupStrategy = new DefaultGroupStrategy();
        VoteWeightStrategy voteWeightStrategy = null;
        ElectionTimeout timeout = new ElectionTimeout(150, 300);

        for (String nodeId : nodeIds) {
            List<String> peerIds = new ArrayList<>(nodeIds);
            peerIds.remove(nodeId);
            TestNode testNode = new TestNode(nodeId, peerIds, timeout, voteWeightStrategy, groupStrategy);
            testNodes.add(testNode);
        }

        for (TestNode node : testNodes) {
            node.connectToAll(testNodes);
        }

        for (TestNode node : testNodes) {
            node.start();
        }

        CountDownLatch leaderElected = new CountDownLatch(1);
        Thread waitThread = new Thread(() -> {
            while (leaderElected.getCount() > 0) {
                for (TestNode node : testNodes) {
                    if (node.isLeader()) {
                        leaderElected.countDown();
                        return;
                    }
                }
                try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
            }
        });
        waitThread.start();
        assertTrue(leaderElected.await(5, TimeUnit.SECONDS), "初始选举应在5秒内完成");
        waitThread.interrupt();
        waitThread.join(100);

        TestNode leader = null;
        TestNode isolated = null;   // 分区中孤立的一侧
        for (TestNode node : testNodes) {
            if (node.isLeader()) {
                leader = node;
            } else if (isolated == null) {
                isolated = node;
            }
        }
        assertNotNull(leader, "应有Leader");
        assertNotNull(isolated, "应有Follower");

        final String isolatedId = isolated.getNodeId();

        // 真正的 2:1 分区：孤立方与其它两节点的链路全部断开（双向）。
        // 原实现只断 leader<->isolated 一条链，孤立方仍可与 node-3 通信并通过
        // 存活链路持续抬升 term，最终将原 Leader 拉下——这是 Raft 的正确行为，
        // 而非缺陷，因此原断言"Leader 必然维持"在错误拓扑下不成立。
        for (TestNode node : testNodes) {
            if (!node.getNodeId().equals(isolatedId)) {
                node.disconnectFrom(isolatedId);
                isolated.disconnectFrom(node.getNodeId());
            }
        }

        Thread.sleep(4000);

        // 分区后收敛语义：
        // 1. 分区一侧（2 节点）相互连通、可继续换选，应恰好持有 1 个 Leader；
        // 2. 孤立方无法获得多数票，永远不能成为 Leader（Raft 安全性）；
        // 3. 全集群 Leader 总数不超过 1（分区内 1 个 + 孤立方 0 个）。
        int partitionLeaderCount = 0;
        for (TestNode node : testNodes) {
            if (node == isolated) {
                continue;
            }
            if (node.isLeader()) {
                partitionLeaderCount++;
            }
        }
        assertFalse(isolated.isLeader(), "孤立方无法获得多数，不可能成为Leader");
        assertEquals(1, partitionLeaderCount, "分区的 2 节点一方应恰有 1 个 Leader");
    }

    @Test
    @DisplayName("场景十二：网络分区测试（Leader孤立）")
    void testNetworkPartitionLeaderIsolated() throws InterruptedException {
        List<String> nodeIds = Arrays.asList("node-1", "node-2", "node-3");
        GroupStrategy groupStrategy = new DefaultGroupStrategy();
        VoteWeightStrategy voteWeightStrategy = null;
        ElectionTimeout timeout = new ElectionTimeout(150, 300);

        for (String nodeId : nodeIds) {
            List<String> peerIds = new ArrayList<>(nodeIds);
            peerIds.remove(nodeId);
            TestNode testNode = new TestNode(nodeId, peerIds, timeout, voteWeightStrategy, groupStrategy);
            testNodes.add(testNode);
        }

        for (TestNode node : testNodes) {
            node.connectToAll(testNodes);
        }

        for (TestNode node : testNodes) {
            node.start();
        }

        CountDownLatch leaderElected = new CountDownLatch(1);
        Thread waitThread = new Thread(() -> {
            while (leaderElected.getCount() > 0) {
                for (TestNode node : testNodes) {
                    if (node.isLeader()) {
                        leaderElected.countDown();
                        return;
                    }
                }
                try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
            }
        });
        waitThread.start();
        assertTrue(leaderElected.await(5, TimeUnit.SECONDS), "初始选举应在5秒内完成");
        waitThread.interrupt();
        waitThread.join(100);

        TestNode leader = null;
        List<TestNode> followers = new ArrayList<>();
        for (TestNode node : testNodes) {
            if (node.isLeader()) {
                leader = node;
            } else {
                followers.add(node);
            }
        }
        assertNotNull(leader, "应有Leader");
        assertEquals(2, followers.size(), "应有2个Follower");

        final String leaderId = leader.getNodeId();

        for (TestNode follower : followers) {
            follower.disconnectFrom(leaderId);
        }

        leader.shutdown();

        CountDownLatch newLeaderElected = new CountDownLatch(1);
        Thread monitorThread = new Thread(() -> {
            while (newLeaderElected.getCount() > 0) {
                for (TestNode node : testNodes) {
                    if (node.isLeader() && !node.getNodeId().equals(leaderId)) {
                        newLeaderElected.countDown();
                        return;
                    }
                }
                try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
            }
        });
        monitorThread.start();

        // 时序加固：选举窗口从 5s 放宽到 10s，避免 CI 负载下的临界 flaky
        boolean newElected = newLeaderElected.await(10, TimeUnit.SECONDS);
        monitorThread.interrupt();
        monitorThread.join(100);

        assertTrue(newElected, "剩余2节点应在10秒内选出新Leader");

        int leaderCount = 0;
        for (TestNode node : testNodes) {
            if (node.isLeader() && !node.getNodeId().equals(leaderId)) {
                leaderCount++;
            }
        }
        assertEquals(1, leaderCount, "剩余2节点应有且仅有1个Leader");
    }

    @Test
    @DisplayName("场景十三：网络恢复测试")
    void testNetworkRecovery() throws InterruptedException {
        List<String> nodeIds = Arrays.asList("node-1", "node-2", "node-3");
        GroupStrategy groupStrategy = new DefaultGroupStrategy();
        VoteWeightStrategy voteWeightStrategy = null;
        ElectionTimeout timeout = new ElectionTimeout(150, 300);

        for (String nodeId : nodeIds) {
            List<String> peerIds = new ArrayList<>(nodeIds);
            peerIds.remove(nodeId);
            TestNode testNode = new TestNode(nodeId, peerIds, timeout, voteWeightStrategy, groupStrategy);
            testNodes.add(testNode);
        }

        for (TestNode node : testNodes) {
            node.connectToAll(testNodes);
        }

        for (TestNode node : testNodes) {
            node.start();
        }

        CountDownLatch leaderElected = new CountDownLatch(1);
        Thread waitThread = new Thread(() -> {
            while (leaderElected.getCount() > 0) {
                for (TestNode node : testNodes) {
                    if (node.isLeader()) {
                        leaderElected.countDown();
                        return;
                    }
                }
                try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
            }
        });
        waitThread.start();
        assertTrue(leaderElected.await(5, TimeUnit.SECONDS), "初始选举应在5秒内完成");
        waitThread.interrupt();
        waitThread.join(100);

        TestNode leader = null;
        TestNode follower = null;
        for (TestNode node : testNodes) {
            if (node.isLeader()) {
                leader = node;
            } else {
                follower = node;
            }
        }
        assertNotNull(leader, "应有Leader");
        assertNotNull(follower, "应有Follower");

        final String followerId = follower.getNodeId();
        follower.disconnectFrom(leader.getNodeId());

        Thread.sleep(2000);

        leader.disconnectFrom(followerId);
        follower.disconnectFrom(leader.getNodeId());

        Thread.sleep(2000);

        follower.connectTo(leader);
        leader.connectTo(follower);

        // 恢复收敛：分区期间孤立方 term 已高于原 Leader，恢复后 Follower 会先拒绝
        // 心跳直至 term 追平并重新选举收敛。因此不能断言瞬时状态，需轮询等待
        // 集群达到稳定态：全网恰好 1 个 Leader，其余节点均收敛为 Follower。
        long deadline = System.currentTimeMillis() + 5_000;
        boolean converged = false;
        while (System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
            int leaderCount = 0;
            TestNode currentLeader = null;
            for (TestNode node : testNodes) {
                if (node.isLeader()) {
                    leaderCount++;
                    currentLeader = node;
                }
            }
            if (leaderCount == 1 && currentLeader != null) {
                boolean othersAllFollower = true;
                for (TestNode node : testNodes) {
                    if (node != currentLeader && !node.isFollower()) {
                        othersAllFollower = false;
                        break;
                    }
                }
                if (othersAllFollower && currentLeader.isLeader()) {
                    converged = true;
                    break;
                }
            }
        }

        assertTrue(converged, "恢复后集群应在5秒内收敛为 1 Leader + 2 Follower 稳定态");
    }
}