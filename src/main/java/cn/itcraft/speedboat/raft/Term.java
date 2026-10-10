package cn.itcraft.speedboat.raft;

/**
 * Term 类。
 * 
 * @author speedboat
 * @since 1.0.0
 */
public class Term {
    
    // 报告002 C-2：计数接口 RaftNode.getTerm 声明为"volatile 快照读"，
    // 外部线程（Speedboat.getTerm 等观测 API）可任意线程读——非 volatile 在
    // 契约上是失真。写仍在 raft 单线程内。
    private volatile long current;
    
    public Term() {
        this.current = 0;
    }
    
    public long getCurrent() {
        return current;
    }
    
    public void increment() {
        current++;
    }
    
    public boolean updateIfHigher(long newTerm) {
        if (newTerm > current) {
            current = newTerm;
            return true;
        }
        return false;
    }
    
    public boolean isMonotonicViolation(long proposedTerm) {
        return proposedTerm < current;
    }
    
    public void reset() {
        current = 0;
    }
}