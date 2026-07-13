package cn.itcraft.speedboat.raft;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LeaderRecordTest {

    @Test
    void testConstruction() {
        LeaderRecord record = new LeaderRecord(5, "node1");
        
        assertEquals(5, record.getTerm());
        assertEquals("node1", record.getLeaderId());
    }

    @Test
    void testToString() {
        LeaderRecord record = new LeaderRecord(5, "node1");
        String str = record.toString();
        
        assertTrue(str.contains("term=5"));
        assertTrue(str.contains("node1"));
    }
}