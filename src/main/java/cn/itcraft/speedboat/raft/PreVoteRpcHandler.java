package cn.itcraft.speedboat.raft;
import cn.itcraft.speedboat.transport.TransportLayer;

/**
 * PreVote 入站处理器（内部协作器，非公开 API）。
 *
 * <p>职责唯一：注册传输层预投票回调并搬回 raft 单线程受理
 * （三拒绝规则本体在 {@code ElectionCoordinator.doHandlePreVoteRequest}）。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
final class PreVoteRpcHandler {

    private final TransportLayer transport;
    private final ElectionCoordinator election;
    private final RpcBridge bridge;

    PreVoteRpcHandler(TransportLayer transport, ElectionCoordinator election, RpcBridge bridge) {
        this.transport = transport;
        this.election = election;
        this.bridge = bridge;
    }

    /** 挂载（start 期一次） */
    void register() {
        transport.setPreVoteHandler(request -> bridge.submit(() -> election.doHandlePreVoteRequest(request)));
    }
}
