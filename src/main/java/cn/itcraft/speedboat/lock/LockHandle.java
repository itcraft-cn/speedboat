package cn.itcraft.speedboat.lock;

/**
 * LockHandle 接口。
 * 
 * @author speedboat
 * @since 1.0.0
 */
public interface LockHandle extends AutoCloseable {

    boolean isSuccess();

    String getLockName();

    String getNodeId();

    /**
     * fencing token：所有权代际（每次成功持锁的所有权变更 +1）。
     * 下游信凭 {@code lockName + epoch} 单调递增拒绝旧主；
     * 未成功持锁或旧实现返回 -1。
     */
    default long getEpoch() {
        return -1L;
    }

    @Override
    void close();
}