package cn.itcraft.speedboat.lock;

public interface DistributedLock {

    LockHandle tryLock();

    LockHandle tryLock(long timeoutMs);

    void unlock();

    boolean isLocked();

    boolean isHeldByCurrentNode();

    String getLockName();

    String getHolderNodeId();
}