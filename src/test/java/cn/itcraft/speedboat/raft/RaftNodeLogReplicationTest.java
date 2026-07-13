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
}