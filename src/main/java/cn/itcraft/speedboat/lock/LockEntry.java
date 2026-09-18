package cn.itcraft.speedboat.lock;

/**
 * 锁表条目：单把锁的持有态、重入计数、租约与 fencing epoch。
 *
 * <p><b>多持有者建模（2026-09-18 命名锁设计）：</b></p>
 * <ul>
 *   <li>本类只在状态机 apply 路径上 mutate（全部 synchronized 仅防御测试直接调用），
 *       真正的全序化由 Raft 日志保证；</li>
 *   <li>{@code leaseExpireTime} 由 Leader 授权打点传播而来（全网一致值的，
 *       详见 LockCommand grantTimestampMs），本地到期判定用自身墙钟对比——
 *       跨副本仅有 NTP 级漂移，该偏差已文档化；</li>
 *   <li>{@code epoch} 是 fencing token：每次<b>所有权变更</b>（新持有者成功
 *       tryAcquire）+1；重入/续期不推进——下游凭 lockName+epoch 单调拒绝旧主。</li>
 * </ul>
 *
 * @author speedboat
 * @since 1.0.0
 */
public class LockEntry {

    private final String lockName;
    private volatile String nodeId;
    private volatile int holdCount;
    private volatile long leaseExpireTime;
    /** fencing token：所有权代次，每次新持有者成功获取时 +1（重入/续期不变） */
    private volatile long epoch;

    public LockEntry(String lockName) {
        this.lockName = lockName;
        this.nodeId = null;
        this.holdCount = 0;
        this.leaseExpireTime = 0;
        this.epoch = 0;
    }

    public boolean isHeld() {
        return nodeId != null && holdCount > 0;
    }

    public boolean isHeldBy(String requestNodeId) {
        return requestNodeId != null && requestNodeId.equals(nodeId);
    }

    public boolean isLeaseExpired() {
        return leaseExpireTime > 0 && System.currentTimeMillis() > leaseExpireTime;
    }

    /**
     * 申请持有（授权时刻由 Leader 打点，全网一致）。
     *
     * <p>判定顺序：未持有 / 已过期 → 新所有权（epoch+1）；同节点且未过期 → 重入；
     * 其余（他人在持且未过期）→ 拒绝。</p>
     *
     * @param requestNodeId  申请者节点 ID
     * @param leaseTimeoutMs 本轮租约时长
     * @param grantPointMs   Leader 授权打点（毫秒墙钟）
     * @return true=授予成功（调用方应读取 {@link #getEpoch()} 作为 fencing token）
     */
    public synchronized boolean tryAcquire(String requestNodeId, long leaseTimeoutMs, long grantPointMs) {
        if (!isHeld() || isLeaseExpired()) {
            this.nodeId = requestNodeId;
            this.holdCount = 1;
            this.leaseExpireTime = grantPointMs + leaseTimeoutMs;
            this.epoch++;
            return true;
        }

        if (requestNodeId.equals(nodeId)) {
            holdCount++;
            leaseExpireTime = grantPointMs + leaseTimeoutMs;
            return true;
        }

        return false;
    }

    /**
     * 兼容重载：授权时刻取本地墙钟（仅供单机测试/旧调用路径使用；
     * 产品路径必须走 {@link #tryAcquire(String, long, long)} 由 Leader 打点）。
     */
    public synchronized boolean tryAcquire(String requestNodeId, long leaseTimeoutMs) {
        return tryAcquire(requestNodeId, leaseTimeoutMs, System.currentTimeMillis());
    }

    public synchronized boolean release(String requestNodeId) {
        if (!isHeldBy(requestNodeId)) {
            return false;
        }

        holdCount--;
        if (holdCount <= 0) {
            nodeId = null;
            holdCount = 0;
            leaseExpireTime = 0;
            // 释放不消耗 epoch：新持有者获取时才会递增，保证"同一所有权盯着一个 token"
        }

        return true;
    }

    /**
     * 续期（授权时刻由 Leader 打点）。
     *
     * <p>续期失败（他人持有/本方已过期丢失）返回 false——调用方应立即
     * 视为锁失守（宁可误停，不可双持）。</p>
     */
    public synchronized boolean renew(String requestNodeId, long leaseTimeoutMs, long grantPointMs) {
        if (!isHeldBy(requestNodeId)) {
            return false;
        }

        leaseExpireTime = grantPointMs + leaseTimeoutMs;
        return true;
    }

    /** 兼容重载：授权时刻取本地墙钟（仅测试/旧路径） */
    public synchronized boolean renew(String requestNodeId, long leaseTimeoutMs) {
        return renew(requestNodeId, leaseTimeoutMs, System.currentTimeMillis());
    }

    public String getLockName() {
        return lockName;
    }

    public String getNodeId() {
        return nodeId;
    }

    public int getHoldCount() {
        return holdCount;
    }

    public long getLeaseExpireTime() {
        return leaseExpireTime;
    }

    /** fencing token：所有权代际，本次持有期内不变 */
    public long getEpoch() {
        return epoch;
    }

    @Override
    public String toString() {
        return "LockEntry{" +
            "lockName='" + lockName + '\'' +
            ", nodeId='" + nodeId + '\'' +
            ", holdCount=" + holdCount +
            ", leaseExpireTime=" + leaseExpireTime +
            ", epoch=" + epoch +
            '}';
    }
}
