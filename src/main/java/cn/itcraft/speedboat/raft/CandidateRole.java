package cn.itcraft.speedboat.raft;

/**
 * CANDIDATE 角色（内部协作器，非公开 API）。
 *
 * <p>进入副作用（与原 {@code doTransitionTo} CANDIDATE 分支逐字等价）：
 * 清空投票登记 + 自投票 + 当前任期自增（Raft §5.2：Candidate 开局必自投）。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
final class CandidateRole implements RaftRole {

    private final NodeContext ctx;

    CandidateRole(NodeContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public NodeState state() {
        return NodeState.CANDIDATE;
    }

    @Override
    public void enter(RoleLifecycle lifecycle) {
        ctx.votesReceived.clear();
        ctx.votedFor = ctx.nodeId;
        ctx.term.increment();
    }
}
