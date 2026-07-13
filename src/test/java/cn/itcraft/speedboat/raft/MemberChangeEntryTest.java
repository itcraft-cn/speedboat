package cn.itcraft.speedboat.raft;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MemberChangeEntryTest {

    @Test
    void testAddConstruction() {
        MemberChangeEntry entry = MemberChangeEntry.add(1, 5, "leader1", "node2", "127.0.0.1:8080");
        
        assertEquals(1, entry.getIndex());
        assertEquals(5, entry.getTerm());
        assertEquals("leader1", entry.getLeaderId());
        assertEquals(MemberChangeEntry.ChangeType.ADD, entry.getChangeType());
        assertEquals("node2", entry.getNodeId());
        assertEquals("127.0.0.1:8080", entry.getAddress());
        assertEquals(LogEntry.EntryType.MEMBER_CHANGE, entry.getEntryType());
    }

    @Test
    void testRemoveConstruction() {
        MemberChangeEntry entry = MemberChangeEntry.remove(2, 6, "leader2", "node3");
        
        assertEquals(2, entry.getIndex());
        assertEquals(6, entry.getTerm());
        assertEquals("leader2", entry.getLeaderId());
        assertEquals(MemberChangeEntry.ChangeType.REMOVE, entry.getChangeType());
        assertEquals("node3", entry.getNodeId());
        assertNull(entry.getAddress());
    }

    @Test
    void testEquals() {
        MemberChangeEntry entry1 = MemberChangeEntry.add(1, 5, "leader1", "node2", "127.0.0.1:8080");
        MemberChangeEntry entry2 = MemberChangeEntry.add(1, 5, "leader1", "node2", "127.0.0.1:8080");
        MemberChangeEntry entry3 = MemberChangeEntry.remove(1, 5, "leader1", "node2");
        MemberChangeEntry entry4 = MemberChangeEntry.add(2, 5, "leader1", "node2", "127.0.0.1:8080");
        
        assertEquals(entry1, entry2);
        assertNotEquals(entry1, entry3);
        assertNotEquals(entry1, entry4);
    }

    @Test
    void testHashCode() {
        MemberChangeEntry entry1 = MemberChangeEntry.add(1, 5, "leader1", "node2", "127.0.0.1:8080");
        MemberChangeEntry entry2 = MemberChangeEntry.add(1, 5, "leader1", "node2", "127.0.0.1:8080");
        
        assertEquals(entry1.hashCode(), entry2.hashCode());
    }

    @Test
    void testToString() {
        MemberChangeEntry entry = MemberChangeEntry.add(1, 5, "leader1", "node2", "127.0.0.1:8080");
        String str = entry.toString();
        
        assertTrue(str.contains("index=1"));
        assertTrue(str.contains("term=5"));
        assertTrue(str.contains("leaderId='leader1'"));
        assertTrue(str.contains("changeType=ADD"));
        assertTrue(str.contains("nodeId='node2'"));
        assertTrue(str.contains("address='127.0.0.1:8080'"));
    }

    @Test
    void testStaticFactoryMethods() {
        MemberChangeEntry addEntry = MemberChangeEntry.add(1, 5, "leader1", "node2", "127.0.0.1:8080");
        assertEquals(MemberChangeEntry.ChangeType.ADD, addEntry.getChangeType());
        
        MemberChangeEntry removeEntry = MemberChangeEntry.remove(2, 6, "leader2", "node3");
        assertEquals(MemberChangeEntry.ChangeType.REMOVE, removeEntry.getChangeType());
        assertNull(removeEntry.getAddress());
    }
}