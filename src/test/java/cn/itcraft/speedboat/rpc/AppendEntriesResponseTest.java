package cn.itcraft.speedboat.rpc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AppendEntriesResponseTest {

    @Test
    void testSuccessResponse() {
        AppendEntriesResponse response = new AppendEntriesResponse(5, true, 10);
        
        assertEquals(5, response.getTerm());
        assertTrue(response.isSuccess());
        assertEquals(10, response.getMatchIndex());
    }

    @Test
    void testFailureResponse() {
        AppendEntriesResponse response = new AppendEntriesResponse(5, false, 5);
        
        assertEquals(5, response.getTerm());
        assertFalse(response.isSuccess());
        assertEquals(5, response.getMatchIndex());
    }
}