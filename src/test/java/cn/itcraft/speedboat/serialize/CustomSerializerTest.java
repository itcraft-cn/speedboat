package cn.itcraft.speedboat.serialize;

import cn.itcraft.speedboat.rpc.*;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("CustomSerializer 测试：编解码正确性 + 帧协议兼容性")
class CustomSerializerTest {

    private CustomSerializer customSerializer;
    private ProtostuffSerializer protostuffSerializer;

    @BeforeEach
    void setUp() {
        protostuffSerializer = new ProtostuffSerializer();
        customSerializer = new CustomSerializer(protostuffSerializer);
    }

    /** 通过 EmbeddedChannel 模拟 Netty 接收端的帧解码 */
    private byte[] passThroughFrameDecoder(byte[] frame) {
        EmbeddedChannel channel = new EmbeddedChannel(
            new LengthFieldBasedFrameDecoder(1024, 0, 4, 0, 4)
        );
        channel.writeInbound(Unpooled.wrappedBuffer(frame));
        io.netty.buffer.ByteBuf buf = channel.readInbound();
        if (buf == null) {
            return null;
        }
        byte[] result = new byte[buf.readableBytes()];
        buf.readBytes(result);
        buf.release();
        return result;
    }

    // ========== 帧格式验证 ==========

    @Test
    @DisplayName("帧格式：length 字段 = payload + 6 (CRC4 + serType1 + msgType1)")
    void testFrameLengthFieldIncludesSubHeader() {
        HeartbeatRequest request = new HeartbeatRequest(1L, "leader-1");
        byte[] frame = customSerializer.wrap(request);

        int lengthField = ByteBuffer.wrap(frame).getInt();
        assertEquals(frame.length - 4, lengthField,
            "length 字段应等于帧总长减去 length 字段本身的4字节");
        byte[] payload = protostuffSerializer.serialize(request);
        assertEquals(payload.length + 6, lengthField,
            "length 字段应等于 payload.length + 6");
    }

    @Test
    @DisplayName("帧格式：各字段偏移正确")
    void testFrameFieldOffsets() {
        HeartbeatRequest request = new HeartbeatRequest(42L, "node-A");
        byte[] frame = customSerializer.wrap(request);

        ByteBuffer buf = ByteBuffer.wrap(frame);
        int lengthField = buf.getInt();   // offset 0-3
        int crc32 = buf.getInt();          // offset 4-7
        int serType = buf.get() & 0xFF;   // offset 8
        int msgType = buf.get() & 0xFF;   // offset 9

        assertEquals(frame.length - 4, lengthField);
        assertEquals(protostuffSerializer.getTypeId(), serType);
        assertEquals(3, msgType, "HeartbeatRequest 的 messageType 应为 3");

        byte[] payload = new byte[buf.remaining()];
        buf.get(payload);
        CRC32 crc = new CRC32();
        crc.update(payload);
        assertEquals((int) crc.getValue(), crc32, "CRC32 应与 payload 匹配");
    }

    // ========== 通过帧解码器的端到端测试 ==========

    @Test
    @DisplayName("端到端：wrap → 帧解码器 → unwrap 还原 HeartbeatRequest")
    void testHeartbeatRequestThroughFrameDecoder() {
        HeartbeatRequest original = new HeartbeatRequest(7L, "leader-7");
        byte[] frame = customSerializer.wrap(original);

        byte[] stripped = passThroughFrameDecoder(frame);
        assertNotNull(stripped, "帧解码器应成功解码");
        assertEquals(frame.length - 4, stripped.length);

        HeartbeatRequest result = customSerializer.unwrap(stripped, HeartbeatRequest.class);
        assertEquals(original.getTerm(), result.getTerm());
        assertEquals(original.getLeaderId(), result.getLeaderId());
    }

    @Test
    @DisplayName("端到端：RequestVoteRequest 通过帧解码器")
    void testRequestVoteRequestThroughFrameDecoder() {
        RequestVoteRequest original = new RequestVoteRequest(3L, "candidate-B", 10);
        byte[] frame = customSerializer.wrap(original);

        byte[] stripped = passThroughFrameDecoder(frame);
        assertNotNull(stripped);

        RequestVoteRequest result = customSerializer.unwrap(stripped, RequestVoteRequest.class);
        assertEquals(original.getTerm(), result.getTerm());
        assertEquals(original.getCandidateId(), result.getCandidateId());
        assertEquals(original.getVoteWeight(), result.getVoteWeight());
    }

    @Test
    @DisplayName("端到端：AppendEntriesRequest 通过帧解码器")
    void testAppendEntriesRequestThroughFrameDecoder() {
        AppendEntriesRequest original = new AppendEntriesRequest(5L, "leader-X",
            0, 0, java.util.Collections.emptyList(), 0);
        byte[] frame = customSerializer.wrap(original);

        byte[] stripped = passThroughFrameDecoder(frame);
        assertNotNull(stripped);

        AppendEntriesRequest result = customSerializer.unwrap(stripped, AppendEntriesRequest.class);
        assertEquals(original.getTerm(), result.getTerm());
        assertEquals(original.getLeaderId(), result.getLeaderId());
    }

    @Test
    @DisplayName("端到端：所有6种消息类型均能 round-trip")
    void testAllMessageTypesRoundTrip() {
        Object[] messages = {
            new RequestVoteRequest(1L, "c1", 5),
            new RequestVoteResponse("r1", 1L, true),
            new HeartbeatRequest(2L, "l1"),
            new HeartbeatResponse("r2", 2L, true),
            new AppendEntriesRequest(3L, "l2", 0, 0, java.util.Collections.emptyList(), 0),
            new AppendEntriesResponse("r3", 3L, true, 0)
        };

        for (Object msg : messages) {
            byte[] frame = customSerializer.wrap(msg);
            byte[] stripped = passThroughFrameDecoder(frame);
            assertNotNull(stripped, "帧解码器应成功解码 " + msg.getClass().getSimpleName());
            Object result = customSerializer.unwrap(stripped, Object.class);
            assertEquals(msg.getClass(), result.getClass(),
                "round-trip 后类型应一致: " + msg.getClass().getSimpleName());
        }
    }

    // ========== CRC32 校验 ==========

    @Test
    @DisplayName("CRC32 校验：篡改 payload 后 unwrap 抛异常")
    void testUnwrapDetectsCrc32Failure() {
        HeartbeatRequest request = new HeartbeatRequest(3L, "leader-3");
        byte[] frame = customSerializer.wrap(request);
        byte[] stripped = passThroughFrameDecoder(frame);

        // 篡改 payload 的第一个字节（偏移6 = CRC4 + ser1 + msg1）
        stripped[6] = (byte) (stripped[6] ^ 0xFF);

        assertThrows(SerializationException.class, () -> {
            customSerializer.unwrap(stripped, HeartbeatRequest.class);
        });
    }

    @Test
    @DisplayName("CRC32 校验：不同 payload 产生不同 CRC")
    void testDifferentPayloadsProduceDifferentCrc() {
        HeartbeatRequest r1 = new HeartbeatRequest(10L, "leader-10");
        HeartbeatRequest r2 = new HeartbeatRequest(20L, "leader-20");

        byte[] frame1 = customSerializer.wrap(r1);
        byte[] frame2 = customSerializer.wrap(r2);

        int crc1 = ByteBuffer.wrap(frame1, 4, 4).getInt();
        int crc2 = ByteBuffer.wrap(frame2, 4, 4).getInt();

        assertNotEquals(crc1, crc2, "不同 payload 的 CRC32 应不同");
    }

    // ========== 边界条件 ==========

    @Test
    @DisplayName("unwrap 拒绝过短数据")
    void testUnwrapRejectsTooShortData() {
        byte[] tooShort = new byte[]{1, 2, 3, 4, 5}; // 5 < SUB_HEADER_LEN(6)
        assertThrows(SerializationException.class, () -> {
            customSerializer.unwrap(tooShort, Object.class);
        });
    }

    @Test
    @DisplayName("wrap 拒绝未注册类型")
    void testWrapRejectsUnregisteredType() {
        assertThrows(SerializationException.class, () -> {
            customSerializer.wrap("not a registered type");
        });
    }

    @Test
    @DisplayName("帧解码器丢弃不完整帧（半包）")
    void testFrameDecoderDropsIncompleteFrame() {
        HeartbeatRequest request = new HeartbeatRequest(1L, "l1");
        byte[] frame = customSerializer.wrap(request);

        // 只发送前半部分
        byte[] half = new byte[frame.length / 2];
        System.arraycopy(frame, 0, half, 0, half.length);

        EmbeddedChannel channel = new EmbeddedChannel(
            new LengthFieldBasedFrameDecoder(1024, 0, 4, 0, 4)
        );
        channel.writeInbound(Unpooled.wrappedBuffer(half));
        Object result = channel.readInbound();
        assertNull(result, "不完整帧应被丢弃");
    }
}
