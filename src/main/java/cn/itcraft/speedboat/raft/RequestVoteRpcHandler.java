package cn.itcraft.speedboat.raft;
import cn.itcraft.speedboat.transport.TransportLayer;

/**
 * RequestVote 入站处理器（内部协作器，非公开 API）。
 *
 * <p>职责唯一：注册传输层投选票回调并把请求搬回 raft 单线程受理——
 * 算法本体在 {@code ElectionCoordinator.doHandleRequestVote}。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
final class RequestVoteRpcHandler {

    private final TransportLayer transport;
    private final ElectionCoordinator election;
    private final RpcBridge bridge;

    RequestVoteRpcHandler(TransportLayer transport, ElectionCoordinator election, RpcBridge bridge) {
        this.transport = transport;
        this.election = election;
        this.bridge = bridge;
    }

    /** 挂载（start 期一次） */
    void register() {
        transport.setRequestVoteHandler(request -> bridge.submit(() -> election.doHandleRequestVote(request)));
    }
}
