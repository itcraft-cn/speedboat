package cn.itcraft.speedboat.raft;

import cn.itcraft.speedboat.rpc.AppendEntriesRequest;
import cn.itcraft.speedboat.rpc.AppendEntriesResponse;
import cn.itcraft.speedboat.rpc.HeartbeatRequest;
import cn.itcraft.speedboat.rpc.HeartbeatResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class RaftNodeLogReplicationTest {

    private RaftNode followerNode;
    private RaftNode leaderNode;

    @BeforeEach
    void setUp() {
        followerNode = new RaftNode.Builder()
            .nodeId("follower1")
            .peerIds(Arrays.asList("leader", "follower2"))
            .build();

        leaderNode = new RaftNode.Builder()
            .nodeId("leader")
            .peerIds(Arrays.asList("follower1", "follower2"))
            .build();
    }

    @Test
    void testInitialLogState() {
        assertEquals(0, followerNode.getCommitIndex());
        assertEquals(0, followerNode.getLastApplied());
        assertTrue(followerNode.getLogEntries().isEmpty());
    }

    @Test
    void testGetLeaderHistoryEmpty() {
        assertTrue(followerNode.getLeaderHistory().isEmpty());
    }

    @Test
    void testFollowerRejectsLowerTerm() {
        followerNode.getTerm().updateIfHigher(5);
        
        AppendEntriesRequest request = new AppendEntriesRequest(
            3, "leader", 0, 0, Collections.emptyList(), 0
        );
        
        AppendEntriesResponse response = followerNode.handleAppendEntries(request);
        
        assertFalse(response.isSuccess());
        assertEquals(5, response.getTerm());
    }

    @Test
    void testFollowerAcceptsHigherTerm() {
        followerNode.getTerm().updateIfHigher(3);
        
        AppendEntriesRequest request = new AppendEntriesRequest(
            5, "leader", 0, 0, Collections.emptyList(), 0
        );
        
        AppendEntriesResponse response = followerNode.handleAppendEntries(request);
        
        assertTrue(response.isSuccess());
        assertEquals(5, followerNode.getTerm().getCurrent());
    }

    @Test
    void testFollowerAppendsEntries() {
        LogEntry entry1 = new LogEntry(1, 5, "leader");
        LogEntry entry2 = new LogEntry(2, 5, "leader");
        
        AppendEntriesRequest request = new AppendEntriesRequest(
            5, "leader", 0, 0, Arrays.asList(entry1, entry2), 0
        );
        
        AppendEntriesResponse response = followerNode.handleAppendEntries(request);
        
        assertTrue(response.isSuccess());
        assertEquals(2, response.getMatchIndex());
        assertEquals(2, followerNode.getLogEntries().size());
    }

    @Test
    void testFollowerRejectsMismatchedPrevLogTerm() {
        LogEntry existingEntry = new LogEntry(1, 3, "oldLeader");
        followerNode.handleAppendEntries(new AppendEntriesRequest(
            3, "oldLeader", 0, 0, Collections.singletonList(existingEntry), 0
        ));
        
        LogEntry newEntry = new LogEntry(2, 5, "leader");
        AppendEntriesRequest request = new AppendEntriesRequest(
            5, "leader", 1, 4, Collections.singletonList(newEntry), 0
        );
        
        AppendEntriesResponse response = followerNode.handleAppendEntries(request);
        
        assertFalse(response.isSuccess());
    }

    @Test
    void testFollowerUpdatesCommitIndex() {
        LogEntry entry = new LogEntry(1, 5, "leader");
        
        AppendEntriesRequest request = new AppendEntriesRequest(
            5, "leader", 0, 0, Collections.singletonList(entry), 1
        );
        
        AppendEntriesResponse response = followerNode.handleAppendEntries(request);
        
        assertTrue(response.isSuccess());
        assertEquals(1, followerNode.getCommitIndex());
    }

    @Test
    void testHeartbeatCompatibility() {
        followerNode.getTerm().updateIfHigher(5);
        
        HeartbeatRequest heartbeat = new HeartbeatRequest(5, "leader");
        HeartbeatResponse response = followerNode.handleHeartbeat(heartbeat);
        
        assertTrue(response.isSuccess());
        assertEquals(5, response.getTerm());
    }

    @Test
    void testLogTruncation() {
        RaftNode node = new RaftNode.Builder()
            .nodeId("node1")
            .maxLogSize(3)
            .build();
        
        node.getTerm().updateIfHigher(5);
        
        for (int i = 1; i <= 5; i++) {
            LogEntry entry = new LogEntry(i, 5, "leader");
            AppendEntriesRequest request = new AppendEntriesRequest(
                5, "leader", i - 1, 5, Collections.singletonList(entry), 0
            );
            node.handleAppendEntries(request);
        }
        
        int size = node.getLogEntries().size();
        assertTrue(size <= 4, "Log size should be <= 4, but was " + size);
    }

    @Test
    void testLeaderInitializesNextIndexOnElection() {
        LogEntry entry = new LogEntry(1, 5, "leader");
        leaderNode.handleAppendEntries(new AppendEntriesRequest(
            5, "leader", 0, 0, Collections.singletonList(entry), 1
        ));
        
        leaderNode.transitionTo(NodeState.CANDIDATE);
        leaderNode.getTerm().increment();
        leaderNode.becomeLeader();
        
        assertEquals(NodeState.LEADER, leaderNode.getCurrentState());
    }

    @Test
    void testLeaderAppendsEntryOnElection() {
        leaderNode.transitionTo(NodeState.CANDIDATE);
        leaderNode.getTerm().increment();
        leaderNode.becomeLeader();
        
        assertFalse(leaderNode.getLogEntries().isEmpty());
        LogEntry firstEntry = leaderNode.getLogEntries().get(0);
        assertEquals("leader", firstEntry.getLeaderId());
    }

    // ==================== expectedNextIndex 快速回退（SOFAJRaft/Ratis 同款协议扩展，2026-09-22）====================

    @Test
    void testFastBackfillHintWhenLogTooShort() {
        // 已有 index 1、2 的日志（term 5）
        followerNode.handleAppendEntries(new AppendEntriesRequest(
            5, "leader", 0, 0, Arrays.asList(new LogEntry(1, 5, "leader"), new LogEntry(2, 5, "leader")), 0));

        // leader 从 prevLogIndex=9 探测（尾部缺失）→ 应返回"日志过短"的快速回退提示
        AppendEntriesRequest request = new AppendEntriesRequest(
            5, "leader", 9, 5, Collections.emptyList(), 0);
        AppendEntriesResponse response = followerNode.handleAppendEntries(request);

        assertFalse(response.isSuccess());
        assertEquals(2, response.getExpectedNextIndex(), "日志太短应建议从 firstLogIndex+1=2 开始重发（本地首条 1）");
    }

    @Test
    void testFastBackfillHintOnTermMismatch() {
        // 本地 term 3 a 2 条（index 1-2）；请求 term 5 的 prevLogIndex=1 但 prevLogTerm=4 → 冲突
        followerNode.handleAppendEntries(new AppendEntriesRequest(
            3, "oldLeader", 0, 0, Arrays.asList(new LogEntry(1, 3, "oldLeader"), new LogEntry(2, 3, "oldLeader")), 0));

        AppendEntriesRequest request = new AppendEntriesRequest(
            5, "leader", 1, 4, Collections.emptyList(), 0);
        AppendEntriesResponse response = followerNode.handleAppendEntries(request);

        assertFalse(response.isSuccess());
        assertEquals(1, response.getExpectedNextIndex(), "term 不一致 → 应提示从本地该任期首条(index 1)回退");
    }

    @Test
    void testFastBackfillHintOnSameIndexConflict() {
        // 本地在 index 2 是 term 3 与请求 term 5 冲突 → 提示从本地该任期首条(index 2)重回退
        followerNode.handleAppendEntries(new AppendEntriesRequest(
            3, "oldLeader", 0, 0, Arrays.asList(new LogEntry(1, 3, "oldLeader"), new LogEntry(2, 3, "oldLeader")), 0));

        AppendEntriesRequest request = new AppendEntriesRequest(
            5, "leader", 2, 5, Collections.emptyList(), 0);
        AppendEntriesResponse response = followerNode.handleAppendEntries(request);

        assertFalse(response.isSuccess());
        assertEquals(1, response.getExpectedNextIndex(), "同 index 冲突应提示从本地该任期区间的首条(index 1)回退");
    }

    @Test
    void testFastBackfillHintWalksSameTermSpan() {
        // 本地 index 1-3 都是 term 3；请求在 index 3 处 term=4 冲突 → 提示回退到该任期区间首条 index 1
        followerNode.handleAppendEntries(new AppendEntriesRequest(
            3, "oldLeader", 0, 0, Arrays.asList(
            new LogEntry(1, 3, "oldLeader"), new LogEntry(2, 3, "oldLeader"), new LogEntry(3, 3, "oldLeader")), 0));

        AppendEntriesRequest request = new AppendEntriesRequest(
            5, "leader", 3, 5, Collections.emptyList(), 0);
        AppendEntriesResponse response = followerNode.handleAppendEntries(request);

        assertFalse(response.isSuccess());
        assertEquals(1, response.getExpectedNextIndex(), "本地 3 条同任期冲突 → 建议从期首条=1 重发");
    }
}
