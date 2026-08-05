package cn.itcraft.speedboat.raft;

/**
 * Term 类。
 * 
 * @author speedboat
 * @since 1.0.0
 */
public class Term {
    
    private long current;
    
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