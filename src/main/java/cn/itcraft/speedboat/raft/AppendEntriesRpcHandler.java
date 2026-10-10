package cn.itcraft.speedboat.raft;
import cn.itcraft.speedboat.transport.TransportLayer;

/**
 * AppendEntries/Heartbeat 入站处理器（内部协作器，非公开 API）。
 *
 * <p>职责唯一：注册传输层日志复制/心跳回调并搬回 raft 单线程受理
 * （受理本体在 {@code InboundAppendHandler}）。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
final class AppendEntriesRpcHandler {

    private final TransportLayer transport;
    private final InboundAppendHandler inbound;
    private final RpcBridge bridge;

    AppendEntriesRpcHandler(TransportLayer transport, InboundAppendHandler inbound, RpcBridge bridge) {
        this.transport = transport;
        this.inbound = inbound;
        this.bridge = bridge;
    }

    /** 挂载（start 期一次） */
    void register() {
        transport.setHeartbeatHandler(request -> bridge.submit(() -> inbound.handleHeartbeat(request)));
        transport.setAppendEntriesHandler(request -> bridge.submit(() -> inbound.handleAppendEntries(request)));
    }
}
