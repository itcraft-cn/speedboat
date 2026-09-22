package cn.itcraft.speedboat.raft;

/**
 * LEADER 角色（内部协作器，非公开 API）。
 *
 * <p>进入副作用：武装周期心跳（由门面实际调度，幂等武装语义）。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
final class LeaderRole implements RaftRole {

    LeaderRole() {
    }

    @Override
    public NodeState state() {
        return NodeState.LEADER;
    }

    @Override
    public void enter(RoleLifecycle lifecycle) {
        lifecycle.startHeartbeat();
    }
}
