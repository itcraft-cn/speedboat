package cn.itcraft.speedboat.rpc;

import cn.itcraft.speedboat.raft.LogEntry;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AppendEntriesRequestTest {

    @Test
    void testConstruction() {
        LogEntry entry1 = new LogEntry(1, 5, "node1");
        LogEntry entry2 = new LogEntry(2, 5, "node1");
        List<LogEntry> entries = Arrays.asList(entry1, entry2);
        
        AppendEntriesRequest request = new AppendEntriesRequest(
            5, "node1", 0, 0, entries, 1
        );
        
        assertEquals(5, request.getTerm());
        assertEquals("node1", request.getLeaderId());
        assertEquals(0, request.getPrevLogIndex());
        assertEquals(0, request.getPrevLogTerm());
        assertEquals(2, request.getEntries().size());
        assertEquals(1, request.getLeaderCommit());
    }

    @Test
    void testEmptyEntries() {
        AppendEntriesRequest request = new AppendEntriesRequest(
            5, "node1", 0, 0, null, 1
        );
        
        assertEquals(0, request.getEntries().size());
    }

    @Test
    void testHeartbeatFactory() {
        AppendEntriesRequest heartbeat = AppendEntriesRequest.heartbeat(
            5, "node1", 0, 0, 1
        );
        
        assertEquals(5, heartbeat.getTerm());
        assertEquals("node1", heartbeat.getLeaderId());
        assertTrue(heartbeat.getEntries().isEmpty());
    }

    @Test
    void testEntriesUnmodifiable() {
        LogEntry entry = new LogEntry(1, 5, "node1");
        AppendEntriesRequest request = new AppendEntriesRequest(
            5, "node1", 0, 0, Collections.singletonList(entry), 1
        );
        
        assertThrows(UnsupportedOperationException.class, () -> 
            request.getEntries().add(new LogEntry(2, 5, "node1"))
        );
    }
}