package cn.itcraft.speedboat.raft;
import cn.itcraft.speedboat.transport.TransportLayer;

/**
 * LockOp 入站处理器（内部协作器，非公开 API）。
 *
 * <p>职责唯一：注册传输层命名锁转发协议回调并搬回 raft 单线程裁决
 * （裁决本体在 {@code LockOpGateway.doHandleLockOp}）。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
final class LockOpRpcHandler {

    private final TransportLayer transport;
    private final LockOpGateway lockOps;
    private final RpcBridge bridge;

    LockOpRpcHandler(TransportLayer transport, LockOpGateway lockOps, RpcBridge bridge) {
        this.transport = transport;
        this.lockOps = lockOps;
        this.bridge = bridge;
    }

    /** 挂载（start 期一次） */
    void register() {
        transport.setLockOpHandler(request -> bridge.submit(() -> lockOps.doHandleLockOp(request)));
    }
}
