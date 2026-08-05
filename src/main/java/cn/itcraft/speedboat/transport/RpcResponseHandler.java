package cn.itcraft.speedboat.transport;

import cn.itcraft.speedboat.rpc.*;
import cn.itcraft.speedboat.serialize.CustomSerializer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.CompletableFuture;

/**
 * RPC 响应处理器，处理 RPC 响应消息。
 * 
 * <p>继承 {@link ChannelInboundHandlerAdapter}，负责反序列化响应并完成对应的 {@link CompletableFuture}。</p>
 * 
 * @author speedboat
 * @since 1.0.0
 */
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