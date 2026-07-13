package cn.itcraft.speedboat.raft;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class FailureRecord {

    private final String nodeId;
    private final long firstFailureTime;
    private final AtomicInteger failureCount;
    private volatile boolean confirmed;

    public FailureRecord(String nodeId) {
        this.nodeId = nodeId;
        this.firstFailureTime = System.nanoTime();
        this.failureCount = new AtomicInteger(1);
        this.confirmed = false;
    }

    public String getNodeId() {
        return nodeId;
    }

    public long getFirstFailureTime() {
        return firstFailureTime;
    }

    public int getFailureCount() {
        return failureCount.get();
    }

    public void incrementFailureCount() {
        failureCount.incrementAndGet();
    }
    
    public void incrementFailures() {
        incrementFailureCount();
    }

    public boolean isConfirmed() {
        return confirmed;
    }

    public void setConfirmed(boolean confirmed) {
        this.confirmed = confirmed;
    }

    public void reset() {
        failureCount.set(0);
        confirmed = false;
    }

    public long getDurationNanos() {
        return System.nanoTime() - firstFailureTime;
    }

    public boolean shouldProposeRemoval(int failureThreshold, long confirmationNanos) {
        return failureCount.get() >= failureThreshold && 
               getDurationNanos() >= confirmationNanos;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FailureRecord that = (FailureRecord) o;
        return Objects.equals(nodeId, that.nodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId);
    }

    @Override
    public String toString() {
        return "FailureRecord{" +
                "nodeId='" + nodeId + '\'' +
                ", failureCount=" + failureCount.get() +
                ", confirmed=" + confirmed +
                ", durationNanos=" + getDurationNanos() +
                '}';
    }
}