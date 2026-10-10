package cn.itcraft.speedboat.raft;

import java.util.concurrent.ThreadLocalRandom;

/**
 * ElectionTimeout 类。
 * 
 * @author speedboat
 * @since 1.0.0
 */
public class ElectionTimeout {
    
    private final long minMs;
    private final long maxMs;
    private long currentTimeout;
    
    public ElectionTimeout(long minMs, long maxMs) {
        if (minMs < 0 || maxMs < minMs) {
            throw new IllegalArgumentException(
                "ElectionTimeout requires 0 <= minMs <= maxMs, got min=" + minMs + ", max=" + maxMs);
        }
        this.minMs = minMs;
        this.maxMs = maxMs;
        reset();
    }
    
    public long getNext() {
        return currentTimeout;
    }
    
    public void reset() {
        // span == 0（min==max 配置缺口，报告002 M-1）时退化为固定超时，
        // 不得调用 nextLong(0)（ThreadLocalRandom 会抛 IllegalArgumentException）
        long span = maxMs - minMs;
        currentTimeout = span > 0 ? minMs + ThreadLocalRandom.current().nextLong(span) : minMs;
    }
    
    public long getMinMs() { 
        return minMs; 
    }
    
    public long getMaxMs() { 
        return maxMs; 
    }
}