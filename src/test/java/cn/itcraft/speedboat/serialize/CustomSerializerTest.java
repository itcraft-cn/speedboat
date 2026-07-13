package cn.itcraft.speedboat.serialize;

import cn.itcraft.speedboat.rpc.HeartbeatRequest;
import cn.itcraft.speedboat.rpc.RequestVoteRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("CustomSerializer测试")
class CustomSerializerTest {

    private CustomSerializer customSerializer;
    private ProtostuffSerializer protostuffSerializer;

    @BeforeEach
    void setUp() {
        protostuffSerializer = new ProtostuffSerializer();
        customSerializer = new CustomSerializer(protostuffSerializer);
    }

    @Test
    @DisplayName("wrap和unwrap HeartbeatRequest")
    void testWrapAndUnwrapHeartbeatRequest() {
        HeartbeatRequest original = new HeartbeatRequest(1L, "leader-1");
        
        byte[] wrapped = customSerializer.wrap(original);
        assertNotNull(wrapped);
        assertTrue(wrapped.length > 9);
        
        HeartbeatRequest unwrapped = customSerializer.unwrap(wrapped, HeartbeatRequest.class);
        assertNotNull(unwrapped);
        assertEquals(original.getTerm(), unwrapped.getTerm());
        assertEquals(original.getLeaderId(), unwrapped.getLeaderId());
    }

    @Test
    @DisplayName("wrap和unwrap RequestVoteRequest")
    void testWrapAndUnwrapRequestVoteRequest() {
        RequestVoteRequest original = new RequestVoteRequest(2L, "candidate-2", 15);
        
        byte[] wrapped = customSerializer.wrap(original);
        assertNotNull(wrapped);
        assertTrue(wrapped.length > 9);
        
        RequestVoteRequest unwrapped = customSerializer.unwrap(wrapped, RequestVoteRequest.class);
        assertNotNull(unwrapped);
        assertEquals(original.getTerm(), unwrapped.getTerm());
        assertEquals(original.getCandidateId(), unwrapped.getCandidateId());
        assertEquals(original.getVoteWeight(), unwrapped.getVoteWeight());
    }

    @Test
    @DisplayName("wrap包含正确的头部信息")
    void testWrapContainsCorrectHeader() {
        HeartbeatRequest request = new HeartbeatRequest(5L, "leader-5");
        byte[] wrapped = customSerializer.wrap(request);
        
        assertEquals(protostuffSerializer.getTypeId(), wrapped[8] & 0xFF);
        assertTrue(wrapped.length > 9);
    }

    @Test
    @DisplayName("unwrap检测到CRC32校验失败")
    void testUnwrapDetectsCrc32Failure() {
        HeartbeatRequest request = new HeartbeatRequest(3L, "leader-3");
        byte[] wrapped = customSerializer.wrap(request);
        
        wrapped[5] = (byte) (wrapped[5] ^ 0xFF);
        
        assertThrows(SerializationException.class, () -> {
            customSerializer.unwrap(wrapped, HeartbeatRequest.class);
        });
    }

    @Test
    @DisplayName("unwrap检测到无效数据长度")
    void testUnwrapDetectsInvalidDataLength() {
        byte[] invalidData = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
        
        assertThrows(SerializationException.class, () -> {
            customSerializer.unwrap(invalidData, HeartbeatRequest.class);
        });
    }

    @Test
    @DisplayName("多次wrap产生不同的CRC32")
    void testMultipleWrapProducesDifferentCrc32() {
        HeartbeatRequest request1 = new HeartbeatRequest(10L, "leader-10");
        HeartbeatRequest request2 = new HeartbeatRequest(20L, "leader-20");
        
        byte[] wrapped1 = customSerializer.wrap(request1);
        byte[] wrapped2 = customSerializer.wrap(request2);
        
        int crc1 = ((wrapped1[4] & 0xFF) << 24) | ((wrapped1[5] & 0xFF) << 16) | 
                   ((wrapped1[6] & 0xFF) << 8) | (wrapped1[7] & 0xFF);
        int crc2 = ((wrapped2[4] & 0xFF) << 24) | ((wrapped2[5] & 0xFF) << 16) | 
                   ((wrapped2[6] & 0xFF) << 8) | (wrapped2[7] & 0xFF);
        
        assertNotEquals(crc1, crc2);
    }
}
