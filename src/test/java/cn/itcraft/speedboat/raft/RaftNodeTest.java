package cn.itcraft.speedboat.raft;
import cn.itcraft.speedboat.rpc.AppendEntriesRequest;
import cn.itcraft.speedboat.rpc.AppendEntriesResponse;
import cn.itcraft.speedboat.rpc.HeartbeatRequest;
import cn.itcraft.speedboat.rpc.HeartbeatResponse;
import cn.itcraft.speedboat.rpc.LockOpRequest;
import cn.itcraft.speedboat.rpc.LockOpResponse;
import cn.itcraft.speedboat.rpc.PreVoteRequest;
import cn.itcraft.speedboat.rpc.PreVoteResponse;
import cn.itcraft.speedboat.rpc.RequestVoteRequest;
import cn.itcraft.speedboat.rpc.RequestVoteResponse;
import cn.itcraft.speedboat.strategy.group.GroupStrategy;
import cn.itcraft.speedboat.strategy.voteweight.VoteWeightStrategy;
import cn.itcraft.speedboat.transport.TransportLayer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
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
    @DisplayName("投票权重 - 候选者机房取请求方自报值，而非本节点机房")
    void testVoteWeightUsesRequestDatacenter() {
        // 记录策略实际看到的候选者机房，断言上下文传递的是请求方机房
        List<String> seenCandidateDatacenters = new ArrayList<>();
        VoteWeightStrategy dcAware = context -> {
            seenCandidateDatacenters.add(context.getCandidateDatacenter());
            // 同机房 +2 / 异机房 -1：据此可区分传入的到底是哪个机房
            return "dc-A".equals(context.getCandidateDatacenter()) ? 2 : -1;
        };

        RaftNode dcNode = new RaftNode.Builder()
            .nodeId("node-1")
            .peerIds(Arrays.asList("node-2", "node-3"))
            .electionTimeout(new ElectionTimeout(150, 300))
            .datacenter("dc-A")
            .voteWeightStrategy(dcAware)
            .transportLayer(new TestTransportLayer())
            .build();

        // 请求方自报 dc-B（异机房）→ 应取 -1；若误取本节点机房 dc-A 则会得到 +2
        assertEquals(0, dcNode.calculateVoteWeight(new RequestVoteRequest(1, "node-2", 1, "dc-B")));
        assertEquals("dc-B", seenCandidateDatacenters.get(seenCandidateDatacenters.size() - 1));

        // 请求方自报 dc-A（同机房）→ 应取 +2
        assertEquals(3, dcNode.calculateVoteWeight(new RequestVoteRequest(1, "node-3", 1, "dc-A")));

        // 请求方未自报机房（旧版本节点）→ 回退为本节点机房，保持既有行为不变
        assertEquals(3, dcNode.calculateVoteWeight(new RequestVoteRequest(1, "node-2", 1)));
    }

    // ==================== 跨机房父组：代表席位门控 ====================

    @Test
    @DisplayName("席位门控 - 未持有席位的节点拒绝投票（防止同机房多份代表虚增票数）")
    void seatGateRejectsVoteWhenNoSeat() {
        node.setSeatHeld(false);

        RequestVoteResponse response = node.handleRequestVote(new RequestVoteRequest(1, "candidate-1", 1));

        assertFalse(response.isVoteGranted(), "无席位节点不代表本机房投票");
    }

    @Test
    @DisplayName("席位门控 - 恢复席位后重新参与投票")
    void seatGateRestoresVotingWhenSeatGranted() {
        node.setSeatHeld(false);
        assertFalse(node.handleRequestVote(new RequestVoteRequest(1, "candidate-1", 1)).isVoteGranted());

        node.setSeatHeld(true);

        assertTrue(node.handleRequestVote(new RequestVoteRequest(1, "candidate-1", 1)).isVoteGranted());
    }

    @Test
    @DisplayName("席位门控 - 缺省持有席位，单机房行为不变")
    void seatDefaultsToHeld() {
        assertTrue(node.isSeatHeld(), "缺省必须持有席位，否则会静默破坏所有既有单机房集群");
    }

    @Test
    @DisplayName("席位门控 - 失去席位时若已是 LEADER 则主动退位")
    void seatGateStepsDownLeaderWhenSeatRevoked() throws InterruptedException {
        transport.setVoteResponse(true);
        node.start();
        Thread.sleep(400);
        assertEquals(NodeState.LEADER, node.getCurrentState());

        node.setSeatHeld(false);

        // 退位在 raft 单线程上异步执行，稍等再断言
        Thread.sleep(100);
        assertEquals(NodeState.FOLLOWER, node.getCurrentState(),
            "失去代表资格后不得继续以 Leader 身份对外发心跳");
        node.shutdown();
    }

    @Test
    @DisplayName("席位门控 - 未持有席位的节点不发起选举")
    void seatGateBlocksElectionWithoutSeat() throws InterruptedException {
        transport.setVoteResponse(true);
        node.setSeatHeld(false);
        node.start();

        // 若门控失效，本节点会在此窗口内竞选并登基（transport 已放行投票）
        Thread.sleep(500);

        assertEquals(NodeState.FOLLOWER, node.getCurrentState(), "无席位不得竞选");
        assertEquals(0, node.getTerm().getCurrent(), "无席位不得推进 term");
        node.shutdown();
    }

    @Test
    @DisplayName("席位门控 - 公开入口 startElection / becomeLeader 不得越过席位")
    void seatGateBlocksForcedElectionAndLeadershipWithoutSeat() throws InterruptedException {
        transport.setVoteResponse(true);
        node.setSeatHeld(false);
        node.start();

        // startElection() / becomeLeader() 绕过选举定时器，是"父组 learner 被直接推上
        // Leader"的唯一通道；全网互联拓扑下学习者自身权重已达门槛，故必须在此把关。
        node.startElection();
        Thread.sleep(300);
        assertEquals(NodeState.FOLLOWER, node.getCurrentState(), "startElection() 不得越席位");
        assertEquals(0, node.getTerm().getCurrent(), "startElection() 不得推进 term");

        node.becomeLeader();
        Thread.sleep(200);
        assertFalse(node.isLeader(), "becomeLeader() 不得越席位");

        // 门控不是永久封禁：恢复席位后公开入口重新生效
        node.setSeatHeld(true);
        node.startElection();
        Thread.sleep(300);
        assertTrue(node.getCurrentState() == NodeState.CANDIDATE || node.isLeader(),
            "持席位后 startElection() 应重新生效");
        node.shutdown();
    }

    @Test
    @DisplayName("席位门控 - 预投票同样受席位门控（预票不得绕过代表资格）")
    void seatGateRejectsPreVoteWhenNoSeat() throws Exception {
        node.setSeatHeld(false);
        node.start();

        // 预投票经 RpcBridge 注册到传输层处理器，可从测试桩直接驱动
        TransportLayer.PreVoteHandler handler = transport.preVoteHandler;
        assertNotNull(handler, "start() 应注册预投票处理器");

        // 探测 term 取 100：本节点选举最多推进到 1，故绝不会命中"陈旧探测"规则，
        // 使断言严格落在席位门控上而非被任期比较掩盖。
        PreVoteResponse rejected = handler
            .handle(new PreVoteRequest(100, "candidate-1", 0, 0, "dc-0"))
            .get(2, TimeUnit.SECONDS);
        assertFalse(rejected.isVoteGranted(), "无席位节点不代表本机房应答预票");

        node.setSeatHeld(true);
        PreVoteResponse granted = handler
            .handle(new PreVoteRequest(100, "candidate-1", 0, 0, "dc-0"))
            .get(2, TimeUnit.SECONDS);
        assertTrue(granted.isVoteGranted(), "持有席位时预票应放行，否则会静默掐死跨机房选举");
        node.shutdown();
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
        private TransportLayer.PreVoteHandler preVoteHandler;

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
        public CompletableFuture<LockOpResponse> sendLockOp(String peerId, LockOpRequest request) {
            return CompletableFuture.completedFuture(new LockOpResponse(request.getRequestId(), false, -1));
        }

        @Override
        public void setLockOpHandler(TransportLayer.LockOpHandler handler) {
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
        public void setPreVoteHandler(TransportLayer.PreVoteHandler handler) {
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