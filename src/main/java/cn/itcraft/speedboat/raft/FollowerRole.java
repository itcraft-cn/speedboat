package cn.itcraft.speedboat.raft;

/**
 * FOLLOWER 角色（内部协作器，非公开 API）。
 *
 * <p>进入副作用：撤销周期心跳 + 重置选举超时（回收下一次选举窗口的
 * 随机超时）。linger 一份与原 {@code doTransitionTo} FOLLOWER 分支逐字等价
 * 的语义，唯一的差别是执行者从上帝类拆出为独立角色类。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
final class FollowerRole implements RaftRole {

    private final NodeContext ctx;

    FollowerRole(NodeContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public NodeState state() {
        return NodeState.FOLLOWER;
    }

    @Override
    public void enter(RoleLifecycle lifecycle) {
        lifecycle.cancelHeartbeat();
        lifecycle.resetElectionTimeout();
    }
}
