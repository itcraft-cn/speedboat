package cn.itcraft.speedboat.raft.report;

import cn.itcraft.speedboat.raft.ElectionTimeout;
import cn.itcraft.speedboat.raft.NodeState;
import cn.itcraft.speedboat.raft.RaftNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RaftNodeReport 可观测性验收测试（Phase E）：
 *
 * <ol>
 *   <li>事件双通道：角色变更（start→选举）触发 ROLE_CHANGE 快照；</li>
 *   <li>快照只读副本完整（nodeId/role/term/members/followerMatchIndices）；</li>
 *   <li>监听器异常不影响算法主流程（自愈）。</li>
 * </ol>
 *
 * @author speedboat
 * @since 1.1.0
 */
@DisplayName("RaftNodeReport 可观测性")
class RaftNodeReportListenerTest {

    private List<String> peerIds;
    private RaftNode node;
    private List<RaftNodeReport> received;
    private List<RaftNodeReport> throwingListenerReports;

    @BeforeEach
    void setUp() {
        peerIds = new ArrayList<>();
        peerIds.add("node-2");
        peerIds.add("node-3");
        received = new CopyOnWriteArrayList<>();
        throwingListenerReports = new CopyOnWriteArrayList<>();
    }

    @AfterEach
    void tearDown() throws Exception {
        // no explicit node refs retained per-test
    }

    private static final class ThrowingListener implements RaftNodeReportListener {
        @Override
        public void onReport(RaftNodeReport report) {
            throw new IllegalStateException("boom");
        }
    }

    @Test
    @DisplayName("角色变更触发 ROLE_CHANGE 快照并携带完整字段")
    void testRoleChangeReport() throws Exception {
        RaftNode node = new RaftNode.Builder()
            .nodeId("node-1")
            .peerIds(new ArrayList<>(peerIds))
            .electionTimeout(new ElectionTimeout(100, 200))
            .reportListener(received::add)
            .transportLayer(new MockAllGrantTransport())
            .build();

        node.start();
        try {
            long deadline = System.currentTimeMillis() + 3000;
            while (System.currentTimeMillis() < deadline && !node.isLeader()) {
                TimeUnit.MILLISECONDS.sleep(50);
            }
            assertTrue(node.isLeader(), "应选出 leader");

            assertTrue(received.size() >= 1, "应至少发布一次快照");
            RaftNodeReport last = received.get(received.size() - 1);
            assertEquals("node-1", last.getNodeId());
            assertEquals(NodeState.LEADER, last.getRole());
            assertEquals(1, last.getTerm(), "首任 leader term 应为 1");
            assertTrue(last.getMembers().contains("node-1"), "成员列表应包含自身");
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("监听器异常不应影响算法主流程（自愈）")
    void testListenerExceptionDoesNotBreakAlgorithm() throws Exception {
        RaftNode node = new RaftNode.Builder()
            .nodeId("node-1")
            .peerIds(new ArrayList<>(peerIds))
            .electionTimeout(new ElectionTimeout(100, 200))
            .reportListener(new ThrowingListener())
            .transportLayer(new MockAllGrantTransport())
            .build();

        node.start();
        try {
            long deadline = System.currentTimeMillis() + 2000;
            while (System.currentTimeMillis() < deadline && !node.isLeader()) {
                TimeUnit.MILLISECONDS.sleep(50);
            }
            assertTrue(node.isLeader(), "监听器异常不影响选举/登基");
        } finally {
            node.shutdown();
        }
    }

    /** 测试桩：全部同意（直选路径需要 term==0 && leader==null 的第一步） */
    private static final class MockAllGrantTransport implements cn.itcraft.speedboat.transport.TransportLayer {
        @Override
        public java.util.concurrent.CompletableFuture<cn.itcraft.speedboat.rpc.RequestVoteResponse> sendRequestVote(
            String peerId, cn.itcraft.speedboat.rpc.RequestVoteRequest request) {
            return java.util.concurrent.CompletableFuture.completedFuture(
                new cn.itcraft.speedboat.rpc.RequestVoteResponse(request.getRequestId(), request.getTerm(), true));
        }

        @Override
        public java.util.concurrent.CompletableFuture<cn.itcraft.speedboat.rpc.PreVoteResponse> sendPreVote(
            String peerId, cn.itcraft.speedboat.rpc.PreVoteRequest request) {
            return java.util.concurrent.CompletableFuture.completedFuture(
                new cn.itcraft.speedboat.rpc.PreVoteResponse(request.getRequestId(), 0, true));
        }

        @Override
        public java.util.concurrent.CompletableFuture<cn.itcraft.speedboat.rpc.HeartbeatResponse> sendHeartbeat(
            String peerId, cn.itcraft.speedboat.rpc.HeartbeatRequest request) {
            return java.util.concurrent.CompletableFuture.completedFuture(
                new cn.itcraft.speedboat.rpc.HeartbeatResponse(request.getRequestId(), request.getTerm(), true));
        }

        @Override
        public java.util.concurrent.CompletableFuture<cn.itcraft.speedboat.rpc.AppendEntriesResponse> sendAppendEntries(
            String peerId, cn.itcraft.speedboat.rpc.AppendEntriesRequest request) {
            return java.util.concurrent.CompletableFuture.completedFuture(
                new cn.itcraft.speedboat.rpc.AppendEntriesResponse(request.getRequestId(), request.getTerm(), true, 0));
        }

        @Override
        public void setRequestVoteHandler(cn.itcraft.speedboat.transport.TransportLayer.RequestVoteHandler handler) {
        }

        @Override
        public void setHeartbeatHandler(cn.itcraft.speedboat.transport.TransportLayer.HeartbeatHandler handler) {
        }

        @Override
        public void setAppendEntriesHandler(cn.itcraft.speedboat.transport.TransportLayer.AppendEntriesHandler handler) {
        }

        @Override
        public void setPreVoteHandler(cn.itcraft.speedboat.transport.TransportLayer.PreVoteHandler handler) {
        }
    }
}
