package cn.itcraft.speedboat.transport;

import cn.itcraft.speedboat.rpc.*;
import cn.itcraft.speedboat.serialize.CustomSerializer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    
    private static final Logger logger = LoggerFactory.getLogger(RpcResponseHandler.class);
    private final NettyTransport transport;
    private final CustomSerializer serializer;
    
    RpcResponseHandler(NettyTransport transport, CustomSerializer serializer) {
        this.transport = transport;
        this.serializer = serializer;
    }
    
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        byte[] data;
        if (msg instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) msg;
            data = new byte[buf.readableBytes()];
            buf.readBytes(data);
            buf.release();
        } else {
            data = (byte[]) msg;
        }
        
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
            logger.error("RpcResponseHandler error: {}", e.toString(), e);
            ctx.close();
        }
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("RpcResponseHandler exceptionCaught: {}", cause.toString(), cause);
        ctx.close();
    }
}