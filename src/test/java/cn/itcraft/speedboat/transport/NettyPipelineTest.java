package cn.itcraft.speedboat.transport;

import cn.itcraft.speedboat.rpc.*;
import cn.itcraft.speedboat.serialize.CustomSerializer;
import cn.itcraft.speedboat.serialize.ProtostuffSerializer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class NettyPipelineTest {

    private final CustomSerializer serializer = new CustomSerializer(new ProtostuffSerializer());
    private final LengthFieldBasedFrameDecoder decoder = new LengthFieldBasedFrameDecoder(1024, 0, 4, 0, 4);

    @Test
    void requestVoteRequest通过Pipeline后能正确解码() throws Exception {
        RequestVoteRequest original = new RequestVoteRequest(5L, "node-1", 10);
        byte[] frame = serializer.wrap(original);

        // 模拟 Netty 接收：LengthFieldBasedFrameDecoder 输出 ByteBuf，不是 byte[]
        io.netty.channel.embedded.EmbeddedChannel ch = new io.netty.channel.embedded.EmbeddedChannel(decoder);
        ch.writeInbound(Unpooled.wrappedBuffer(frame));
        ByteBuf decoded = ch.readInbound();
        assertNotNull(decoded, "帧解码器应输出 ByteBuf");

        // 关键：必须先 ByteBuf→byte[]，再 unwrap
        byte[] data = new byte[decoded.readableBytes()];
        decoded.readBytes(data);
        decoded.release();

        RequestVoteRequest result = serializer.unwrap(data, RequestVoteRequest.class);
        assertEquals(original.getTerm(), result.getTerm());
        assertEquals(original.getCandidateId(), result.getCandidateId());
    }

    @Test
    void appendEntriesRequest通过Pipeline后能正确解码() throws Exception {
        AppendEntriesRequest original = new AppendEntriesRequest(
            3L, "leader-1", 10, 2, Collections.emptyList(), 5);
        byte[] frame = serializer.wrap(original);

        io.netty.channel.embedded.EmbeddedChannel ch = new io.netty.channel.embedded.EmbeddedChannel(decoder);
        ch.writeInbound(Unpooled.wrappedBuffer(frame));
        ByteBuf decoded = ch.readInbound();
        assertNotNull(decoded);

        byte[] data = new byte[decoded.readableBytes()];
        decoded.readBytes(data);
        decoded.release();

        AppendEntriesRequest result = serializer.unwrap(data, AppendEntriesRequest.class);
        assertEquals(original.getTerm(), result.getTerm());
        assertEquals(original.getLeaderId(), result.getLeaderId());
    }

    @Test
    void heartbeatRequest通过Pipeline后能正确解码() throws Exception {
        HeartbeatRequest original = new HeartbeatRequest(1L, "leader-1");
        byte[] frame = serializer.wrap(original);

        io.netty.channel.embedded.EmbeddedChannel ch = new io.netty.channel.embedded.EmbeddedChannel(decoder);
        ch.writeInbound(Unpooled.wrappedBuffer(frame));
        ByteBuf decoded = ch.readInbound();
        assertNotNull(decoded);

        byte[] data = new byte[decoded.readableBytes()];
        decoded.readBytes(data);
        decoded.release();

        HeartbeatRequest result = serializer.unwrap(data, HeartbeatRequest.class);
        assertEquals(original.getTerm(), result.getTerm());
        assertEquals(original.getLeaderId(), result.getLeaderId());
    }
}
