package cn.itcraft.speedboat.rpc;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RpcTest {

    @Test
    void testRequestVoteRequest() {
        RequestVoteRequest request = new RequestVoteRequest(1L, "node-1", 5);
        
        assertEquals(1L, request.getTerm());
        assertEquals("node-1", request.getCandidateId());
        assertEquals(5, request.getVoteWeight());
    }

    @Test
    void testRequestVoteResponse() {
        RequestVoteResponse response = new RequestVoteResponse(1L, true);
        
        assertEquals(1L, response.getTerm());
        assertTrue(response.isVoteGranted());
    }

    @Test
    void testRequestVoteResponseVoteDenied() {
        RequestVoteResponse response = new RequestVoteResponse(2L, false);
        
        assertEquals(2L, response.getTerm());
        assertFalse(response.isVoteGranted());
    }

    @Test
    void testHeartbeatRequest() {
        HeartbeatRequest request = new HeartbeatRequest(3L, "leader-1");
        
        assertEquals(3L, request.getTerm());
        assertEquals("leader-1", request.getLeaderId());
    }

    @Test
    void testHeartbeatResponse() {
        HeartbeatResponse response = new HeartbeatResponse(3L, true);
        
        assertEquals(3L, response.getTerm());
        assertTrue(response.isSuccess());
    }

    @Test
    void testHeartbeatResponseFailure() {
        HeartbeatResponse response = new HeartbeatResponse(4L, false);
        
        assertEquals(4L, response.getTerm());
        assertFalse(response.isSuccess());
    }
}
