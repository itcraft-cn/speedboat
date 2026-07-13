package cn.itcraft.speedboat.transport;

import cn.itcraft.speedboat.rpc.*;
import cn.itcraft.speedboat.serialize.CustomSerializer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.CompletableFuture;

class RpcResponseHandler extends ChannelInboundHandlerAdapter {
    
    private final NettyTransport transport;
    private final CustomSerializer serializer;
    
    RpcResponseHandler(NettyTransport transport, CustomSerializer serializer) {
        this.transport = transport;
        this.serializer = serializer;
    }
    
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        byte[] data = (byte[]) msg;
        
        try {
            Object obj = serializer.unwrap(data, Object.class);
            
            if (obj instanceof RpcResponse) {
                RpcResponse response = (RpcResponse) obj;
                String requestId = response.getRequestId();
                
                if (requestId != null) {
                    CompletableFuture<RpcResponse> future = 
                        transport.getPendingRequests().remove(requestId);
                    
                    if (future != null && !future.isDone()) {
                        future.complete(response);
                    }
                }
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