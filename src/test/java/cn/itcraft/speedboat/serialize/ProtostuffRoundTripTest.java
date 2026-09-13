package cn.itcraft.speedboat.serialize;

import cn.itcraft.speedboat.rpc.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ProtostuffRoundTripTest {

    private final ProtostuffSerializer payloadSerializer = new ProtostuffSerializer();
    private final CustomSerializer serializer = new CustomSerializer(payloadSerializer);

    /**
     * 模拟 LengthFieldBasedFrameDecoder 的工作：读取前 4 字节 length 字段后将其剥离，
     * 返回完整帧的剩余部分。unwrap 的契约输入即为此数据（见 CustomSerializer.unwrap javadoc），
     * 直接把 wrap 的完整帧传给 unwrap 会因 payload 多出 4 字节而 CRC 校验失败。
     */
    private static byte[] simulateFrameDecode(byte[] fullFrame) {
        assertTrue(fullFrame.length > 4, "frame must contain length field");
        byte[] stripped = new byte[fullFrame.length - 4];
        System.arraycopy(fullFrame, 4, stripped, 0, stripped.length);
        return stripped;
    }

    @Test
    void requestVoteRoundTrip() throws Exception {
        RequestVoteRequest req = new RequestVoteRequest(5, "node-1", 1);
        String originalRequestId = req.getRequestId();
        assertNotNull(originalRequestId);

        byte[] data = serializer.wrap(req);
        Object obj = serializer.unwrap(simulateFrameDecode(data), Object.class);

        assertInstanceOf(RequestVoteRequest.class, obj);
        RequestVoteRequest deserialized = (RequestVoteRequest) obj;
        assertEquals(5, deserialized.getTerm());
        assertEquals("node-1", deserialized.getCandidateId());
        assertEquals(1, deserialized.getVoteWeight());
        assertEquals(originalRequestId, deserialized.getRequestId());
    }

    @Test
    void requestVoteResponseRoundTrip() throws Exception {
        RequestVoteResponse resp = new RequestVoteResponse("test-id-123", 3, true);
        byte[] data = serializer.wrap(resp);
        Object obj = serializer.unwrap(simulateFrameDecode(data), Object.class);

        assertInstanceOf(RequestVoteResponse.class, obj);
        RequestVoteResponse deserialized = (RequestVoteResponse) obj;
        assertEquals("test-id-123", deserialized.getRequestId());
        assertEquals(3, deserialized.getTerm());
        assertTrue(deserialized.isVoteGranted());
    }

    @Test
    void heartbeatRoundTrip() throws Exception {
        HeartbeatRequest req = new HeartbeatRequest(7, "leader-1");
        String originalRequestId = req.getRequestId();
        byte[] data = serializer.wrap(req);
        Object obj = serializer.unwrap(simulateFrameDecode(data), Object.class);

        assertInstanceOf(HeartbeatRequest.class, obj);
        HeartbeatRequest deserialized = (HeartbeatRequest) obj;
        assertEquals(7, deserialized.getTerm());
        assertEquals("leader-1", deserialized.getLeaderId());
        assertEquals(originalRequestId, deserialized.getRequestId());
    }

    @Test
    void heartbeatResponseRoundTrip() throws Exception {
        HeartbeatResponse resp = new HeartbeatResponse("hb-id-456", 2, true);
        byte[] data = serializer.wrap(resp);
        Object obj = serializer.unwrap(simulateFrameDecode(data), Object.class);

        assertInstanceOf(HeartbeatResponse.class, obj);
        HeartbeatResponse deserialized = (HeartbeatResponse) obj;
        assertEquals("hb-id-456", deserialized.getRequestId());
        assertEquals(2, deserialized.getTerm());
        assertTrue(deserialized.isSuccess());
    }

    @Test
    void appendEntriesRoundTrip() throws Exception {
        AppendEntriesRequest req = AppendEntriesRequest.heartbeat(4, "leader-2", 10, 3, 10);
        String originalRequestId = req.getRequestId();
        byte[] data = serializer.wrap(req);
        Object obj = serializer.unwrap(simulateFrameDecode(data), Object.class);

        assertInstanceOf(AppendEntriesRequest.class, obj);
        AppendEntriesRequest deserialized = (AppendEntriesRequest) obj;
        assertEquals(4, deserialized.getTerm());
        assertEquals("leader-2", deserialized.getLeaderId());
        assertEquals(originalRequestId, deserialized.getRequestId());
    }

    @Test
    void appendEntriesResponseRoundTrip() throws Exception {
        AppendEntriesResponse resp = new AppendEntriesResponse("ae-id-789", 5, true, 20);
        byte[] data = serializer.wrap(resp);
        Object obj = serializer.unwrap(simulateFrameDecode(data), Object.class);

        assertInstanceOf(AppendEntriesResponse.class, obj);
        AppendEntriesResponse deserialized = (AppendEntriesResponse) obj;
        assertEquals("ae-id-789", deserialized.getRequestId());
        assertEquals(5, deserialized.getTerm());
        assertTrue(deserialized.isSuccess());
        assertEquals(20, deserialized.getMatchIndex());
    }
}
