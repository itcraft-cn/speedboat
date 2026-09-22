package cn.itcraft.speedboat.raft;

/**
 * 角色生命周期挂点（内部协作器，非公开 API）。
 *
 * <p>由门面（RaftNodeImpl）实现：心跳武装/撤销、选举超时重置。
 * 保持"策略与执行分离"——角色类只声明副作用意图，执行细节归门面统一调度。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
interface RoleLifecycle {

    /** 武装周期心跳（幂等） */
    void startHeartbeat();

    /** 撤销周期心跳 */
    void cancelHeartbeat();

    /** 重置选举超时（re-arm 随机窗口 + self-rescheduling 检测） */
    void resetElectionTimeout();
}
