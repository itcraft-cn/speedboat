package cn.itcraft.speedboat.persistence;

import cn.itcraft.speedboat.raft.CommandLogEntry;
import cn.itcraft.speedboat.raft.ElectionTimeout;
import cn.itcraft.speedboat.raft.LogEntry;
import cn.itcraft.speedboat.raft.RaftNode;
import cn.itcraft.speedboat.rpc.AppendEntriesRequest;
import cn.itcraft.speedboat.rpc.AppendEntriesResponse;
import cn.itcraft.speedboat.rpc.HeartbeatRequest;
import cn.itcraft.speedboat.rpc.HeartbeatResponse;
import cn.itcraft.speedboat.rpc.PreVoteRequest;
import cn.itcraft.speedboat.rpc.PreVoteResponse;
import cn.itcraft.speedboat.rpc.RequestVoteRequest;
import cn.itcraft.speedboat.rpc.RequestVoteResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RaftStore 契约验收测试（Phase D 持久化接口前置）：
 *
 * <ol>
 *   <li>InMemoryRaftStore：term/votedFor persist→restore 往返一致；</li>
 *   <li>日志 persist/truncate/restore 往返一致；</li>
 *   <li>RaftNode 接线：选举完成后 store 内可见“先持久化后可见”的任期状态。</li>
 * </ol>
 *
 * @author speedboat
 * @since 1.1.0
 */
@DisplayName("RaftStore 持久化契约")
class RaftStoreContractTest {

    private InMemoryRaftStore store;

    @BeforeEach
    void setUp() {
        store = new InMemoryRaftStore();
    }

    @AfterEach
    void tearDown() {
        store = null;
    }

    @Test
    @DisplayName("term/votedFor persist 与 restore 往返一致")
    void testTermPersistRestoreRoundTrip() {
        assertNull(store.restoreTerm(), "未持久化前 restoreTerm 应为 null");

        store.persistAndFlushTerm(5, "node-9", "node-8");
        RaftTermRecord record = store.restoreTerm();
        assertNotNull(record, "持久化后 restoreTerm 应返回记录");
        assertEquals(5, record.getTerm());
        assertEquals("node-9", record.getVotedFor());
        assertEquals("node-8", record.getLeaderId());

        store.persistAndFlushTerm(7, "node-2", "node-2");
        RaftTermRecord latest = store.restoreTerm();
        assertEquals(7, latest.getTerm());
        assertEquals("node-2", latest.getVotedFor());
    }

    @Test
    @DisplayName("日志 persist/truncate/restore 往返一致")
    void testLogPersistTruncateRestoreRoundTrip() {
        List<LogEntry> entries = new ArrayList<>();
        for (long i = 1; i <= 5; i++) {
            entries.add(CommandLogEntry.create(i, 1, "leader-1", ("cmd-" + i).getBytes()));
        }
        store.persistLogEntries(entries);
        assertEquals(5, store.logEntryCount());

        List<LogEntry> restored = store.restoredLogEntries();
        assertEquals(5, restored.size());
        for (int i = 0; i < restored.size(); i++) {
            assertEquals(i + 1, restored.get(i).getIndex());
        }

        store.truncateLogEntriesFrom(4);
        assertEquals(3, store.logEntryCount());

        store.truncateLogEntriesUntil(2);
        assertEquals(1, store.logEntryCount());
        assertEquals(3, store.restoredLogEntries().get(0).getIndex());

        store.deleteSnapshotChunks();
        store.flush();
    }

    @Test
    @DisplayName("RaftNode 与 store 接线：选举完成后 store 内可见任期状态")
    void testRaftNodeWiringPersistsTermOnVote() throws Exception {
        List<String> peerIds = new ArrayList<>();
        peerIds.add("node-2");
        peerIds.add("node-3");

        RaftNode node = new RaftNode.Builder()
            .nodeId("node-1")
            .peerIds(peerIds)
            .electionTimeout(new ElectionTimeout(120, 240))
            .raftStore(store)
            .transportLayer(new MockTransportForTest())
            .build();

        node.start();
        try {
            long deadline = System.currentTimeMillis() + 3000;
            while (System.currentTimeMillis() < deadline && node.getTerm().getCurrent() < 1) {
                TimeUnit.MILLISECONDS.sleep(50);
            }

            assertTrue(node.getTerm().getCurrent() >= 1, "应完成首次选举（term>=1）");
            RaftTermRecord record = store.restoreTerm();
            assertNotNull(record, "选举后 store 应已持久化任期状态");
            assertTrue(record.getTerm() >= 1, "store 中的 term 应 >= 1");
            assertEquals("node-1", record.getVotedFor(), "候选者应已把票投给自己并落盘");
        } finally {
            node.shutdown();
        }
    }

    /**
     * 测试用 TransportLayer 桩：同意预投票/投票/心跳/日志请求，
     * 用于验证 RaftNode 与 RaftStore 的“先持久化后可见”接线。
     */
    private static class MockTransportForTest implements cn.itcraft.speedboat.transport.TransportLayer {
        @Override
        public CompletableFuture<RequestVoteResponse> sendRequestVote(String peerId, RequestVoteRequest request) {
            return CompletableFuture.completedFuture(
                new RequestVoteResponse(request.getRequestId(), request.getTerm(), true));
        }

        @Override
        public CompletableFuture<PreVoteResponse> sendPreVote(String peerId, PreVoteRequest request) {
            return CompletableFuture.completedFuture(
                new PreVoteResponse(request.getRequestId(), 0, true));
        }

        @Override
        public CompletableFuture<HeartbeatResponse> sendHeartbeat(String peerId, HeartbeatRequest request) {
            return CompletableFuture.completedFuture(
                new HeartbeatResponse(request.getRequestId(), request.getTerm(), true));
        }

        @Override
        public CompletableFuture<AppendEntriesResponse> sendAppendEntries(String peerId, AppendEntriesRequest request) {
            return CompletableFuture.completedFuture(
                new AppendEntriesResponse(request.getRequestId(), request.getTerm(), true, 0));
        }

        @Override
        public void setRequestVoteHandler(RequestVoteHandler handler) {
        }

        @Override
        public void setHeartbeatHandler(HeartbeatHandler handler) {
        }

        @Override
        public void setAppendEntriesHandler(AppendEntriesHandler handler) {
        }

        @Override
        public void setPreVoteHandler(PreVoteHandler handler) {
        }
    }
}
