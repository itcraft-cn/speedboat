package cn.itcraft.speedboat.serialize;

import cn.itcraft.speedboat.rpc.HeartbeatRequest;
import cn.itcraft.speedboat.rpc.RequestVoteRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ProtostuffSerializer测试")
class ProtostuffSerializerTest {

    private ProtostuffSerializer serializer;

    @BeforeEach
    void setUp() {
        serializer = new ProtostuffSerializer();
    }

    @Test
    @DisplayName("序列化和反序列化HeartbeatRequest")
    void testSerializeAndDeserializeHeartbeatRequest() {
        HeartbeatRequest original = new HeartbeatRequest(1L, "leader-1");
        
        byte[] data = serializer.serialize(original);
        assertNotNull(data);
        assertTrue(data.length > 0);
        
        HeartbeatRequest deserialized = serializer.deserialize(data, HeartbeatRequest.class);
        assertNotNull(deserialized);
        assertEquals(original.getTerm(), deserialized.getTerm());
        assertEquals(original.getLeaderId(), deserialized.getLeaderId());
    }

    @Test
    @DisplayName("序列化和反序列化RequestVoteRequest")
    void testSerializeAndDeserializeRequestVoteRequest() {
        RequestVoteRequest original = new RequestVoteRequest(2L, "candidate-2", 10);
        
        byte[] data = serializer.serialize(original);
        assertNotNull(data);
        assertTrue(data.length > 0);
        
        RequestVoteRequest deserialized = serializer.deserialize(data, RequestVoteRequest.class);
        assertNotNull(deserialized);
        assertEquals(original.getTerm(), deserialized.getTerm());
        assertEquals(original.getCandidateId(), deserialized.getCandidateId());
        assertEquals(original.getVoteWeight(), deserialized.getVoteWeight());
    }

    @Test
    @DisplayName("序列化null对象返回空数组")
    void testSerializeNull() {
        byte[] data = serializer.serialize(null);
        assertNotNull(data);
        assertEquals(0, data.length);
    }

    @Test
    @DisplayName("反序列化null或空数据返回null")
    void testDeserializeNull() {
        assertNull(serializer.deserialize(null, HeartbeatRequest.class));
        assertNull(serializer.deserialize(new byte[0], HeartbeatRequest.class));
    }

    @Test
    @DisplayName("getTypeId返回2")
    void testGetTypeId() {
        assertEquals(2, serializer.getTypeId());
    }

    @Test
    @DisplayName("序列化相同对象产生相同字节")
    void testSerializeSameObjectProducesSameBytes() {
        HeartbeatRequest request = new HeartbeatRequest(3L, "leader-3");
        
        byte[] data1 = serializer.serialize(request);
        byte[] data2 = serializer.serialize(request);
        
        assertArrayEquals(data1, data2);
    }
}
