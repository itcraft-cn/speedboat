package cn.itcraft.speedboat.raft;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LogEntryTest {

    @Test
    void testConstruction() {
        LogEntry entry = new LogEntry(1, 5, "node1");
        
        assertEquals(1, entry.getIndex());
        assertEquals(5, entry.getTerm());
        assertEquals("node1", entry.getLeaderId());
        assertEquals(LogEntry.EntryType.LEADER_INFO, entry.getEntryType());
    }

    @Test
    void testEquals() {
        LogEntry entry1 = new LogEntry(1, 5, "node1");
        LogEntry entry2 = new LogEntry(1, 5, "node1");
        LogEntry entry3 = new LogEntry(2, 5, "node1");
        
        assertEquals(entry1, entry2);
        assertNotEquals(entry1, entry3);
    }

    @Test
    void testEqualsWithMemberChange() {
        LogEntry leaderEntry = new LogEntry(1, 5, "node1");
        MemberChangeEntry memberEntry = MemberChangeEntry.add(1, 5, "node1", "node2", "127.0.0.1:8080");
        
        assertNotEquals(leaderEntry, memberEntry);
    }

    @Test
    void testHashCode() {
        LogEntry entry1 = new LogEntry(1, 5, "node1");
        LogEntry entry2 = new LogEntry(1, 5, "node1");
        
        assertEquals(entry1.hashCode(), entry2.hashCode());
    }

    @Test
    void testToString() {
        LogEntry entry = new LogEntry(1, 5, "node1");
        String str = entry.toString();
        
        assertTrue(str.contains("index=1"));
        assertTrue(str.contains("term=5"));
        assertTrue(str.contains("node1"));
        assertTrue(str.contains("entryType"));
    }

    @Test
    void testEntryType() {
        LogEntry leaderEntry = new LogEntry(1, 5, "node1");
        assertEquals(LogEntry.EntryType.LEADER_INFO, leaderEntry.getEntryType());
        
        MemberChangeEntry memberEntry = MemberChangeEntry.add(2, 5, "node1", "node3", "127.0.0.1:8081");
        assertEquals(LogEntry.EntryType.MEMBER_CHANGE, memberEntry.getEntryType());
    }
}