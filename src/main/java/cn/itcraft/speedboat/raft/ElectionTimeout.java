package cn.itcraft.speedboat.raft;

import java.util.concurrent.ThreadLocalRandom;

public class ElectionTimeout {
    
    private final long minMs;
    private final long maxMs;
    private long currentTimeout;
    
    public ElectionTimeout(long minMs, long maxMs) {
        this.minMs = minMs;
        this.maxMs = maxMs;
        reset();
    }
    
    public long getNext() {
        return currentTimeout;
    }
    
    public void reset() {
        currentTimeout = minMs + ThreadLocalRandom.current().nextLong(maxMs - minMs);
    }
    
    public long getMinMs() { 
        return minMs; 
    }
    
    public long getMaxMs() { 
        return maxMs; 
    }
}