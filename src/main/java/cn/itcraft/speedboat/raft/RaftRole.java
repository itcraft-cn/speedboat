package cn.itcraft.speedboat.raft;

/**
 * 角色行为接口（内部协作器，非公开 API；上策角色状态模式）。
 *
 * <p>每种角色（FOLLOWER/CANDIDATE/LEADER）一个实现类，只描述"进入该角色时
 * 的副作用"；角色合法迁移图与生效时点归 {@link RoleMachine} 单管。</p>
 *
 * <p>并发契约：仅限 raft 单线程调用。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
interface RaftRole {

    /** 本角色对应的状态（枚举桥，供 RoleMachine 查表） */
    NodeState state();

    /**
     * 进入角色时的副作用（raft 单线程内调用）。
     *
     * @param lifecycle 生命周期挂点（心跳节拍武装/撤销、选举超时重置）
     */
    void enter(RoleLifecycle lifecycle);
}
