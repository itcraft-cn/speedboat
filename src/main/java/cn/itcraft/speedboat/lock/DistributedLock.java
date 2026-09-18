package cn.itcraft.speedboat.lock;

/**
 * 分布式锁接口，基于 Raft 共识算法提供强一致性的分布式锁服务。
 * 
 * <p>DistributedLock 是 Speedboat 的核心应用层接口，将 Raft 共识能力封装为易用的分布式锁 API，
 * 支持互斥锁、超时获取、锁状态查询等常用功能。</p>
 * 
 * <p>设计原则：</p>
 * <ul>
 *   <li><b>强一致性</b>：基于 Raft 日志复制，确保所有节点对锁状态达成共识</li>
 *   <li><b>容错性</b>：Leader 故障时自动切换，锁状态通过日志恢复</li>
 *   <li><b>非阻塞设计</b>：提供 tryLock 语义，支持超时等待，避免线程阻塞</li>
 *   <li><b>资源安全</b>：锁与持有者节点绑定，节点故障时自动释放</li>
 * </ul>
 * 
 * <p>核心功能：</p>
 * <ol>
 *   <li><b>尝试获取锁</b>：{@link #tryLock()} 立即返回，成功返回 {@link LockHandle}，失败返回 null</li>
 *   <li><b>超时获取锁</b>：{@link #tryLock(long)} 在指定时间内重试获取</li>
 *   <li><b>释放锁</b>：{@link #unlock()} 释放当前节点持有的锁</li>
 *   <li><b>状态查询</b>：{@link #isLocked()} 查询锁是否被任意节点持有</li>
 *   <li><b>持有者查询</b>：{@link #isHeldByCurrentNode()} 查询当前节点是否持有锁</li>
 * </ol>
 * 
 * <p>实现机制：</p>
 * <ul>
 *   <li><b>锁状态机</b>：{@link LockStateMachine} 维护锁状态，通过 Raft 日志复制</li>
 *   <li><b>锁命令</b>：{@link LockCommand} 封装加锁/解锁操作，作为 Raft 日志条目</li>
 *   <li><b>锁句柄</b>：{@link LockHandle} 表示锁的所有权，支持自动释放</li>
 *   <li><b>本地缓存</b>：{@link DistributedLockImpl} 实现本地锁状态缓存，减少 Raft 查询开销</li>
 * </ul>
 * 
 * <p>典型用法：</p>
 * <pre>{@code
 * // 获取分布式锁实例
 * DistributedLock lock = Speedboat.getLock("my-resource");
 * 
 * // 尝试获取锁（立即返回）
 * LockHandle handle = lock.tryLock();
 * if (handle != null) {
 *     try {
 *         // 临界区操作
 *         doCriticalWork();
 *     } finally {
 *         // 确保释放锁
 *         lock.unlock();
 *     }
 * }
 * 
 * // 超时获取锁（最多等待 5 秒）
 * LockHandle handle = lock.tryLock(5000);
 * if (handle != null) {
 *     try {
 *         // 临界区操作
 *     } finally {
 *         lock.unlock();
 *     }
 * }
 * }</pre>
 * 
 * <p>性能特性：</p>
 * <ul>
 *   <li><b>本地化优化</b>：Leader 节点本地缓存锁状态，减少网络往返</li>
 *   <li><b>批量提交</b>：支持批量锁操作，提高吞吐量</li>
 *   <li><b>心跳续期</b>：长锁支持心跳续期，防止误释放</li>
 * </ul>
 * 
 * <p>注意事项：</p>
 * <ul>
 *   <li>分布式锁性能受 Raft 日志复制延迟影响，不适合超高频率锁操作</li>
 *   <li>锁释放必须由持有者节点调用，否则会抛出异常</li>
 *   <li>节点故障时，其持有的锁会在选举超时后自动释放（通过日志恢复）</li>
 *   <li>跨机房场景下，锁操作延迟会增加，需调整超时参数</li>
 * </ul>
 * 
 * @author speedboat
 * @see DistributedLockImpl
 * @see LockStateMachine
 * @see LockCommand
 * @see LockHandle
 * @since 1.0.0
 */
public interface DistributedLock {

    LockHandle tryLock();

    LockHandle tryLock(long timeoutMs);

    void unlock();

    boolean isLocked();

    boolean isHeldByCurrentNode();

    /**
     * 锁是否已在本节点视角失效（可观测出口，P4-M4）。
     *
     * <p>为 true 的场景：{@code RENEW} 被拒（RENEW_LOST）/ 本地 applied 视图已不持有
     * （自然过期或被他节点接管）。业务契约：**发布前应校验本值**；为 true 即必须停止
     * 使用该锁对应的发布权（框架不提供回调，主动轮询是本版本唯一通道）。</p>
     */
    default boolean isLost() {
        return false;
    }

    String getLockName();

    String getHolderNodeId();
}