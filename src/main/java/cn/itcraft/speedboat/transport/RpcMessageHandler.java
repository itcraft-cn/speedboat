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
 * RPC 消息处理器，处理 Netty 通道中的 RPC 消息。
 * 
 * <p>继承 {@link ChannelInboundHandlerAdapter}，负责反序列化和路由 RPC 请求。</p>
 * 
 * @author speedboat
 * @since 1.0.0
 */
class RpcMessageHandler extends ChannelInboundHandlerAdapter {
    
    private static final Logger logger = LoggerFactory.getLogger(RpcMessageHandler.class);
    private final NettyTransport transport;
    private final CustomSerializer serializer;
    
    RpcMessageHandler(NettyTransport transport, CustomSerializer serializer) {
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
            
            if (obj instanceof RequestVoteRequest && transport.getRequestVoteHandler() != null) {
                RequestVoteRequest request = (RequestVoteRequest) obj;
                // 异步派发：handler 返回 Future，完成后回写应答（不再阻塞 Netty IO 线程）
                dispatch(ctx, transport.getRequestVoteHandler().handle(request), request.getRequestId(), false, false);
            } else if (obj instanceof PreVoteRequest && transport.getPreVoteHandler() != null) {
                PreVoteRequest request = (PreVoteRequest) obj;
                dispatch(ctx, transport.getPreVoteHandler().handle(request), request.getRequestId(), false, true);
            } else if (obj instanceof HeartbeatRequest && transport.getHeartbeatHandler() != null) {
                HeartbeatRequest request = (HeartbeatRequest) obj;
                dispatch(ctx, transport.getHeartbeatHandler().handle(request), request.getRequestId(), false, false);
            } else if (obj instanceof AppendEntriesRequest && transport.getAppendEntriesHandler() != null) {
                AppendEntriesRequest request = (AppendEntriesRequest) obj;
                dispatch(ctx, transport.getAppendEntriesHandler().handle(request), request.getRequestId(), true, false);
            } else if (obj instanceof LockOpRequest && transport.getLockOpHandler() != null) {
                LockOpRequest request = (LockOpRequest) obj;
                dispatchLockOp(ctx, transport.getLockOpHandler().handle(request), request.getRequestId());
            }
        } catch (Exception e) {
            logger.error("RpcMessageHandler channelRead error: {}", e.toString(), e);
            ctx.close();
        }
    }

    /**
     * Future 完成后以 requestId 重建并回写应答。ch 不受 future 结果影响。
     *
     * @param isAppendEntries true 表示 AppendEntries 应答（需携带 matchIndex）
     */
    private void dispatch(ChannelHandlerContext ctx, CompletableFuture<? extends RpcResponse> future,
                          String requestId, boolean isAppendEntries, boolean isPreVote) {
        future.whenComplete((response, throwable) -> {
            if (throwable != null || response == null) {
                logger.debug("Inbound request processing failed, no response written: requestId={}", requestId);
                return;
            }
            // 以请求的 requestId 关联回应答，保证发送方可匹配
            try {
                if (isAppendEntries) {
                    AppendEntriesResponse r = (AppendEntriesResponse) response;
                    ctx.writeAndFlush(serializer.wrap(new AppendEntriesResponse(
                        requestId, r.getTerm(), r.isSuccess(), r.getMatchIndex())));
                } else if (isPreVote) {
                    PreVoteResponse r = (PreVoteResponse) response;
                    ctx.writeAndFlush(serializer.wrap(new PreVoteResponse(requestId, r.getTerm(), r.isVoteGranted())));
                } else if (response instanceof HeartbeatResponse) {
                    HeartbeatResponse r = (HeartbeatResponse) response;
                    ctx.writeAndFlush(serializer.wrap(new HeartbeatResponse(requestId, r.getTerm(), r.isSuccess())));
                } else {
                    RequestVoteResponse r = (RequestVoteResponse) response;
                    if (response instanceof RequestVoteResponse) {
                        ctx.writeAndFlush(serializer.wrap(new RequestVoteResponse(
                            requestId, r.getTerm(), r.isVoteGranted())));
                    }
                }
            } catch (Exception e) {
                logger.error("RpcMessageHandler dispatch write error: {}", e.toString(), e);
            }
        });
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("RpcMessageHandler exceptionCaught: {}", cause.toString(), cause);
        ctx.close();
    }

    /**
     * 锁转发请求的应答回写：响应以 requestId 关联（ok + entryIndex）。
     * 判定（GRANTED/DENIED/epoch）不在此传递——发起方从本地 apply 结果读取。
     */
    private void dispatchLockOp(ChannelHandlerContext ctx,
                                java.util.concurrent.CompletableFuture<LockOpResponse> future,
                                String requestId) {
        future.whenComplete((response, throwable) -> {
            if (throwable != null || response == null) {
                logger.debug("Lock op request processing failed, no response written: requestId={}", requestId);
                return;
            }
            try {
                ctx.writeAndFlush(serializer.wrap(new LockOpResponse(requestId, response.isOk(), response.getEntryIndex())));
            } catch (Exception e) {
                logger.error("dispatchLockOp write error: {}", e.toString(), e);
            }
        });
    }
}
