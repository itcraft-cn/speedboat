package cn.itcraft.speedboat.lock;

public class LockEntry {

    private final String lockName;
    private volatile String nodeId;
    private volatile int holdCount;
    private volatile long leaseExpireTime;

    public LockEntry(String lockName) {
        this.lockName = lockName;
        this.nodeId = null;
        this.holdCount = 0;
        this.leaseExpireTime = 0;
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

    public synchronized boolean tryAcquire(String requestNodeId, long leaseTimeoutMs) {
        if (!isHeld() || isLeaseExpired()) {
            this.nodeId = requestNodeId;
            this.holdCount = 1;
            this.leaseExpireTime = System.currentTimeMillis() + leaseTimeoutMs;
            return true;
        }

        if (requestNodeId.equals(nodeId)) {
            holdCount++;
            leaseExpireTime = System.currentTimeMillis() + leaseTimeoutMs;
            return true;
        }

        return false;
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
        }

        return true;
    }

    public synchronized boolean renew(String requestNodeId, long leaseTimeoutMs) {
        if (!isHeldBy(requestNodeId)) {
            return false;
        }

        leaseExpireTime = System.currentTimeMillis() + leaseTimeoutMs;
        return true;
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

    @Override
    public String toString() {
        return "LockEntry{" +
            "lockName='" + lockName + '\'' +
            ", nodeId='" + nodeId + '\'' +
            ", holdCount=" + holdCount +
            ", leaseExpireTime=" + leaseExpireTime +
            '}';
    }
}