package cn.itcraft.speedboat.raft;

import cn.itcraft.speedboat.rpc.AppendEntriesRequest;
import cn.itcraft.speedboat.rpc.AppendEntriesResponse;
import cn.itcraft.speedboat.rpc.HeartbeatRequest;
import cn.itcraft.speedboat.rpc.PreVoteRequest;
import cn.itcraft.speedboat.rpc.PreVoteResponse;
import cn.itcraft.speedboat.rpc.HeartbeatResponse;
import cn.itcraft.speedboat.rpc.RequestVoteRequest;
import cn.itcraft.speedboat.rpc.RequestVoteResponse;
import cn.itcraft.speedboat.strategy.group.GroupStrategy;
import cn.itcraft.speedboat.strategy.voteweight.VoteWeightStrategy;
import cn.itcraft.speedboat.transport.TransportLayer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class RaftNodeTest {

    private RaftNode node;
    private TestTransportLayer transport;
    private TestVoteWeightStrategy voteWeightStrategy;
    private TestGroupStrategy groupStrategy;

    @BeforeEach
    void setUp() {
        transport = new TestTransportLayer();
        voteWeightStrategy = new TestVoteWeightStrategy();
        groupStrategy = new TestGroupStrategy();
        
        node = new RaftNode.Builder()
            .nodeId("node-1")
            .peerIds(Arrays.asList("node-2", "node-3"))
            .electionTimeout(new ElectionTimeout(150, 300))
            .voteWeightStrategy(voteWeightStrategy)
            .groupStrategy(groupStrategy)
            .transportLayer(transport)
            .build();
    }

    @Test
    @DisplayName("初始状态应为FOLLOWER")
    void testInitialState() {
        assertEquals(NodeState.FOLLOWER, node.getCurrentState());
        assertEquals(0, node.getTerm().getCurrent());
        assertNull(node.getVotedFor());
        assertNull(node.getLeaderId());
    }

    @Test
    @DisplayName("启动后应开始选举超时计时")
    void testStartBeginsElectionTimeout() throws InterruptedException {
        node.start();
        Thread.sleep(400);
        assertEquals(NodeState.CANDIDATE, node.getCurrentState());
        node.shutdown();
    }

    @Test
    @DisplayName("超时后应转为CANDIDATE")
    void testTimeoutTransitionsToCandidate() throws InterruptedException {
        node.start();
        Thread.sleep(400);
        assertEquals(NodeState.CANDIDATE, node.getCurrentState());
        assertTrue(node.getTerm().getCurrent() >= 1);
        assertEquals("node-1", node.getVotedFor());
        node.shutdown();
    }

    @Test
    @DisplayName("CANDIDATE获得多数票后应转为LEADER")
    void testCandidateBecomesLeaderWithMajorityVotes() throws InterruptedException {
        transport.setVoteResponse(true);
        
        node.start();
        Thread.sleep(400);
        
        assertEquals(NodeState.LEADER, node.getCurrentState());
        assertTrue(node.isMain());
        node.shutdown();
    }

    @Test
    @DisplayName("收到更高Term心跳应转为FOLLOWER")
    void testHigherTermHeartbeatTransitionsToFollower() throws InterruptedException {
        node.start();
        Thread.sleep(400);
        
        HeartbeatRequest request = new HeartbeatRequest(10, "leader-1");
        HeartbeatResponse response = node.handleHeartbeat(request);
        
        assertTrue(response.isSuccess());
        assertEquals(NodeState.FOLLOWER, node.getCurrentState());
        assertEquals(10, node.getTerm().getCurrent());
        assertEquals("leader-1", node.getLeaderId());
        node.shutdown();
    }

    @Test
    @DisplayName("Term单调性校验 - 拒绝更低Term请求")
    void testRejectLowerTermRequest() {
        node.getTerm().updateIfHigher(5);
        
        RequestVoteRequest request = new RequestVoteRequest(3, "candidate-1", 1);
        RequestVoteResponse response = node.handleRequestVote(request);
        
        assertFalse(response.isVoteGranted());
        assertEquals(5, response.getTerm());
    }

    @Test
    @DisplayName("Term单调性校验 - 接受更高Term请求")
    void testAcceptHigherTermRequest() {
        RequestVoteRequest request = new RequestVoteRequest(10, "candidate-1", 1);
        RequestVoteResponse response = node.handleRequestVote(request);
        
        assertTrue(response.isVoteGranted());
        assertEquals(10, node.getTerm().getCurrent());
        assertEquals("candidate-1", node.getVotedFor());
    }

    @Test
    @DisplayName("投票权重的计算")
    void testVoteWeightCalculation() {
        voteWeightStrategy.setAdditionalWeight(5);
        
        RequestVoteRequest request = new RequestVoteRequest(1, "node-1", 1);
        
        assertEquals(6, node.calculateVoteWeight(request));
    }

    @Test
    @DisplayName("LEADER收到更高Term心跳应转为FOLLOWER")
    void testLeaderReceivesHigherTermHeartbeat() throws InterruptedException {
        transport.setVoteResponse(true);
        node.start();
        Thread.sleep(400);
        
        assertEquals(NodeState.LEADER, node.getCurrentState());
        
        HeartbeatRequest request = new HeartbeatRequest(10, "new-leader");
        HeartbeatResponse response = node.handleHeartbeat(request);
        
        assertTrue(response.isSuccess());
        assertEquals(NodeState.FOLLOWER, node.getCurrentState());
        node.shutdown();
    }

    @Test
    @DisplayName("FOLLOWER收到心跳应保持FOLLOWER状态")
    void testFollowerReceivesHeartbeat() {
        HeartbeatRequest request = new HeartbeatRequest(1, "leader-1");
        HeartbeatResponse response = node.handleHeartbeat(request);
        
        assertTrue(response.isSuccess());
        assertEquals(NodeState.FOLLOWER, node.getCurrentState());
        assertEquals("leader-1", node.getLeaderId());
    }

    @Test
    @DisplayName("已投票的节点不应再次投票")
    void testAlreadyVotedCannotVoteAgain() {
        RequestVoteRequest request1 = new RequestVoteRequest(1, "candidate-1", 1);
        RequestVoteResponse response1 = node.handleRequestVote(request1);
        assertTrue(response1.isVoteGranted());
        
        RequestVoteRequest request2 = new RequestVoteRequest(1, "candidate-2", 1);
        RequestVoteResponse response2 = node.handleRequestVote(request2);
        assertFalse(response2.isVoteGranted());
    }

    @Test
    @DisplayName("关闭节点应停止调度器")
    void testShutdownStopsScheduler() throws InterruptedException {
        node.start();
        node.shutdown();
        
        Thread.sleep(400);
        
        assertEquals(NodeState.FOLLOWER, node.getCurrentState());
    }

    @Test
    @DisplayName("心跳应重置选举超时")
    void testHeartbeatResetsElectionTimeout() throws InterruptedException {
        ElectionTimeout shortTimeout = new ElectionTimeout(100, 150);
        node = new RaftNode.Builder()
            .nodeId("node-1")
            .peerIds(Arrays.asList("node-2", "node-3"))
            .electionTimeout(shortTimeout)
            .voteWeightStrategy(voteWeightStrategy)
            .groupStrategy(groupStrategy)
            .transportLayer(transport)
            .build();
        
        node.start();
        Thread.sleep(50);
        
        for (int i = 0; i < 5; i++) {
            HeartbeatRequest request = new HeartbeatRequest(i + 1, "leader-1");
            node.handleHeartbeat(request);
            Thread.sleep(100);
        }
        
        assertEquals(NodeState.FOLLOWER, node.getCurrentState());
        
        node.shutdown();
    }

    @Test
    @DisplayName("状态转换应遵循规则")
    void testStateTransitionRules() {
        assertTrue(NodeState.FOLLOWER.canTransitionTo(NodeState.CANDIDATE));
        assertTrue(NodeState.CANDIDATE.canTransitionTo(NodeState.LEADER));
        assertTrue(NodeState.LEADER.canTransitionTo(NodeState.FOLLOWER));
        assertFalse(NodeState.LEADER.canTransitionTo(NodeState.CANDIDATE));
    }

    private static class TestTransportLayer implements TransportLayer {
        private boolean voteGranted = false;
        private RequestVoteHandler requestVoteHandler;
        private HeartbeatHandler heartbeatHandler;
        private AppendEntriesHandler appendEntriesHandler;
        private cn.itcraft.speedboat.transport.TransportLayer.PreVoteHandler preVoteHandler;

        void setVoteResponse(boolean granted) {
            this.voteGranted = granted;
        }

        @Override
        public CompletableFuture<RequestVoteResponse> sendRequestVote(String peerId, RequestVoteRequest request) {
            return CompletableFuture.completedFuture(
                new RequestVoteResponse(request.getTerm(), voteGranted)
            );
        }

        @Override
        public CompletableFuture<HeartbeatResponse> sendHeartbeat(String peerId, HeartbeatRequest request) {
            return CompletableFuture.completedFuture(
                new HeartbeatResponse(request.getTerm(), true)
            );
        }

        @Override
        public CompletableFuture<PreVoteResponse> sendPreVote(String peerId, PreVoteRequest request) {
            return CompletableFuture.completedFuture(
                new PreVoteResponse(request.getRequestId(), request.getTerm(), voteGranted)
            );
        }

        @Override
        public CompletableFuture<cn.itcraft.speedboat.rpc.LockOpResponse> sendLockOp(String peerId, cn.itcraft.speedboat.rpc.LockOpRequest request) {
            return CompletableFuture.completedFuture(new cn.itcraft.speedboat.rpc.LockOpResponse(request.getRequestId(), false, -1));
        }

        @Override
        public void setLockOpHandler(cn.itcraft.speedboat.transport.TransportLayer.LockOpHandler handler) {
            // 测试桩：不处理锁转发
        }

        @Override
        public CompletableFuture<AppendEntriesResponse> sendAppendEntries(String peerId, AppendEntriesRequest request) {
            return CompletableFuture.completedFuture(
                new AppendEntriesResponse(request.getTerm(), true, request.getEntries().size())
            );
        }

        @Override
        public void setRequestVoteHandler(RequestVoteHandler handler) {
            this.requestVoteHandler = handler;
        }

        @Override
        public void setHeartbeatHandler(HeartbeatHandler handler) {
            this.heartbeatHandler = handler;
        }

        @Override
        public void setAppendEntriesHandler(AppendEntriesHandler handler) {
            this.appendEntriesHandler = handler;
        }

        @Override
        public void setPreVoteHandler(cn.itcraft.speedboat.transport.TransportLayer.PreVoteHandler handler) {
            this.preVoteHandler = handler;
        }
    }

    private static class TestVoteWeightStrategy implements VoteWeightStrategy {
        private int additionalWeight = 0;

        void setAdditionalWeight(int weight) {
            this.additionalWeight = weight;
        }

        @Override
        public int calculateAdditionalWeight(VoteContext context) {
            return additionalWeight;
        }
    }

    private static class TestGroupStrategy implements GroupStrategy {
        @Override
        public long getElectionTimeout() {
            return 200;
        }

        @Override
        public long getHeartbeatInterval() {
            return 50;
        }

        @Override
        public boolean allowCrossGroupCommunication() {
            return true;
        }

        @Override
        public long getMinElectionTimeout() {
            return 150;
        }

        @Override
        public long getMaxElectionTimeout() {
            return 300;
        }
    }

    @Test
    @DisplayName("Builder nodeId 为 null 应抛出异常")
    void testBuilderNodeIdNull() {
        assertThrows(IllegalArgumentException.class, () -> {
            new RaftNode.Builder()
                .nodeId(null)
                .build();
        });
    }

    @Test
    @DisplayName("Builder 不设置 nodeId 应抛出异常")
    void testBuilderWithoutNodeId() {
        assertThrows(IllegalArgumentException.class, () -> {
            new RaftNode.Builder()
                .peerIds(Arrays.asList("node-2"))
                .build();
        });
    }

    @Test
    @DisplayName("handleRequestVote - 相同候选者再次请求（同Term不能重复投票）")
    void testHandleRequestVoteSameCandidateSameTerm() {
        RequestVoteRequest request1 = new RequestVoteRequest(1, "candidate-1", 1);
        RequestVoteResponse response1 = node.handleRequestVote(request1);
        assertTrue(response1.isVoteGranted());
        
        RequestVoteRequest request2 = new RequestVoteRequest(1, "candidate-1", 1);
        RequestVoteResponse response2 = node.handleRequestVote(request2);
        assertFalse(response2.isVoteGranted());
    }

    @Test
    @DisplayName("handleRequestVote - 更高Term重置投票状态")
    void testHandleRequestVoteHigherTermResetsVote() {
        RequestVoteRequest request1 = new RequestVoteRequest(1, "candidate-1", 1);
        RequestVoteResponse response1 = node.handleRequestVote(request1);
        assertTrue(response1.isVoteGranted());
        
        RequestVoteRequest request2 = new RequestVoteRequest(2, "candidate-2", 1);
        RequestVoteResponse response2 = node.handleRequestVote(request2);
        assertTrue(response2.isVoteGranted());
        assertEquals("candidate-2", node.getVotedFor());
    }

    @Test
    @DisplayName("handleHeartbeat - CANDIDATE收到心跳应转为FOLLOWER")
    void testHandleHeartbeatCandidateToFollower() {
        node.transitionTo(NodeState.CANDIDATE);
        assertEquals(NodeState.CANDIDATE, node.getCurrentState());
        
        HeartbeatRequest request = new HeartbeatRequest(1, "leader-1");
        HeartbeatResponse response = node.handleHeartbeat(request);
        
        assertTrue(response.isSuccess());
        assertEquals(NodeState.FOLLOWER, node.getCurrentState());
    }

    @Test
    @DisplayName("handleHeartbeat - LEADER收到更高Term心跳应转为FOLLOWER")
    void testHandleHeartbeatLeaderToFollower() throws InterruptedException {
        transport.setVoteResponse(true);
        node.start();
        Thread.sleep(400);
        
        assertEquals(NodeState.LEADER, node.getCurrentState());
        
        HeartbeatRequest request = new HeartbeatRequest(10, "new-leader");
        HeartbeatResponse response = node.handleHeartbeat(request);
        
        assertTrue(response.isSuccess());
        assertEquals(NodeState.FOLLOWER, node.getCurrentState());
        node.shutdown();
    }

    @Test
    @DisplayName("transitionTo - LEADER转FOLLOWER应取消心跳")
    void testTransitionLeaderToFollowerCancelsHeartbeat() throws InterruptedException {
        transport.setVoteResponse(true);
        node.start();
        Thread.sleep(400);
        
        assertEquals(NodeState.LEADER, node.getCurrentState());
        
        node.transitionTo(NodeState.FOLLOWER);
        assertEquals(NodeState.FOLLOWER, node.getCurrentState());
        
        node.shutdown();
    }

    @Test
    @DisplayName("shutdown - 未启动时不抛出异常")
    void testShutdownWithoutStart() {
        RaftNode freshNode = new RaftNode.Builder()
            .nodeId("fresh-node")
            .build();
        
        assertDoesNotThrow(() -> freshNode.shutdown());
    }

    @Test
    @DisplayName("Builder - 使用默认ElectionTimeout")
    void testBuilderDefaultElectionTimeout() {
        RaftNode nodeWithDefault = new RaftNode.Builder()
            .nodeId("default-timeout-node")
            .build();
        
        assertNotNull(nodeWithDefault);
        nodeWithDefault.shutdown();
    }

    @Test
    @DisplayName("Builder - 设置所有参数")
    void testBuilderWithAllParameters() {
        ElectionTimeout customTimeout = new ElectionTimeout(200, 400);
        RaftNode fullNode = new RaftNode.Builder()
            .nodeId("full-node")
            .peerIds(Arrays.asList("peer1", "peer2"))
            .electionTimeout(customTimeout)
            .voteWeightStrategy(voteWeightStrategy)
            .groupStrategy(groupStrategy)
            .transportLayer(transport)
            .build();
        
        assertNotNull(fullNode);
        assertEquals("full-node", fullNode.getNodeId());
        fullNode.shutdown();
    }

    @Test
    @DisplayName("Builder - peerIds为null时使用空列表")
    void testBuilderNullPeerIds() {
        RaftNode nodeWithNullPeers = new RaftNode.Builder()
            .nodeId("null-peers-node")
            .peerIds(null)
            .build();
        
        assertNotNull(nodeWithNullPeers);
        nodeWithNullPeers.shutdown();
    }

    @Test
    @DisplayName("start - 重复调用不应抛出异常")
    void testStartMultipleTimes() {
        node.start();
        assertDoesNotThrow(() -> node.start());
        node.shutdown();
    }

    @Test
    @DisplayName("handleRequestVote - 投票后重置选举超时")
    void testHandleRequestVoteResetsElectionTimeout() throws InterruptedException {
        ElectionTimeout shortTimeout = new ElectionTimeout(100, 150);
        RaftNode shortNode = new RaftNode.Builder()
            .nodeId("short-node")
            .electionTimeout(shortTimeout)
            .build();
        
        shortNode.start();
        Thread.sleep(50);
        
        RequestVoteRequest request = new RequestVoteRequest(2, "candidate-1", 1);
        RequestVoteResponse response = shortNode.handleRequestVote(request);
        
        assertTrue(response.isVoteGranted());
        Thread.sleep(100);
        assertEquals(NodeState.FOLLOWER, shortNode.getCurrentState());
        
        shortNode.shutdown();
    }

    @Test
    @DisplayName("handleHeartbeat - 更低Term应返回false")
    void testHandleHeartbeatLowerTerm() {
        node.getTerm().updateIfHigher(5);
        
        HeartbeatRequest request = new HeartbeatRequest(3, "old-leader");
        HeartbeatResponse response = node.handleHeartbeat(request);
        
        assertFalse(response.isSuccess());
        assertEquals(5, response.getTerm());
    }
}