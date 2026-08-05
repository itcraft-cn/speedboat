package cn.itcraft.speedboat.transport;

import cn.itcraft.speedboat.rpc.*;
import cn.itcraft.speedboat.serialize.CustomSerializer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * RPC 消息处理器，处理 Netty 通道中的 RPC 消息。
 * 
 * <p>继承 {@link ChannelInboundHandlerAdapter}，负责反序列化和路由 RPC 请求。</p>
 * 
 * @author speedboat
 * @since 1.0.0
 */
class RpcMessageHandler extends ChannelInboundHandlerAdapter {
    
    private final NettyTransport transport;
    private final CustomSerializer serializer;
    
    RpcMessageHandler(NettyTransport transport, CustomSerializer serializer) {
        this.transport = transport;
        this.serializer = serializer;
    }
    
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        byte[] data = (byte[]) msg;
        
        try {
            Object obj = serializer.unwrap(data, Object.class);
            
            if (obj instanceof RequestVoteRequest && transport.getRequestVoteHandler() != null) {
                RequestVoteRequest request = (RequestVoteRequest) obj;
                RequestVoteResponse response = transport.getRequestVoteHandler().handle(request);
                RequestVoteResponse responseWithId = new RequestVoteResponse(
                    request.getRequestId(), response.getTerm(), response.isVoteGranted());
                ctx.writeAndFlush(serializer.wrap(responseWithId));
            } else if (obj instanceof HeartbeatRequest && transport.getHeartbeatHandler() != null) {
                HeartbeatRequest request = (HeartbeatRequest) obj;
                HeartbeatResponse response = transport.getHeartbeatHandler().handle(request);
                HeartbeatResponse responseWithId = new HeartbeatResponse(
                    request.getRequestId(), response.getTerm(), response.isSuccess());
                ctx.writeAndFlush(serializer.wrap(responseWithId));
            } else if (obj instanceof AppendEntriesRequest && transport.getAppendEntriesHandler() != null) {
                AppendEntriesRequest request = (AppendEntriesRequest) obj;
                AppendEntriesResponse response = transport.getAppendEntriesHandler().handle(request);
                AppendEntriesResponse responseWithId = new AppendEntriesResponse(
                    request.getRequestId(), response.getTerm(), response.isSuccess(), response.getMatchIndex());
                ctx.writeAndFlush(serializer.wrap(responseWithId));
            }
        } catch (Exception e) {
            ctx.close();
        }
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }
}
