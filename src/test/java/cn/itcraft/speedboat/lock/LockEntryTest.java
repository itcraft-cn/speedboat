package cn.itcraft.speedboat.lock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LockEntryTest {

    private LockEntry lockEntry;

    @BeforeEach
    void setUp() {
        lockEntry = new LockEntry("test-lock");
    }

    @Test
    void testInitialState() {
        assertFalse(lockEntry.isHeld());
        assertNull(lockEntry.getNodeId());
        assertEquals(0, lockEntry.getHoldCount());
        assertFalse(lockEntry.isLeaseExpired());
    }

    @Test
    void testTryAcquire() {
        boolean result = lockEntry.tryAcquire("node1", 30000);
        
        assertTrue(result);
        assertTrue(lockEntry.isHeld());
        assertTrue(lockEntry.isHeldBy("node1"));
        assertEquals("node1", lockEntry.getNodeId());
        assertEquals(1, lockEntry.getHoldCount());
    }

    @Test
    void testReentrantAcquire() {
        lockEntry.tryAcquire("node1", 30000);
        
        boolean result = lockEntry.tryAcquire("node1", 30000);
        
        assertTrue(result);
        assertEquals(2, lockEntry.getHoldCount());
    }

    @Test
    void testAcquireByDifferentNodeFails() {
        lockEntry.tryAcquire("node1", 30000);
        
        boolean result = lockEntry.tryAcquire("node2", 30000);
        
        assertFalse(result);
        assertEquals("node1", lockEntry.getNodeId());
    }

    @Test
    void testRelease() {
        lockEntry.tryAcquire("node1", 30000);
        
        boolean result = lockEntry.release("node1");
        
        assertTrue(result);
        assertFalse(lockEntry.isHeld());
        assertNull(lockEntry.getNodeId());
        assertEquals(0, lockEntry.getHoldCount());
    }

    @Test
    void testReleaseByDifferentNodeFails() {
        lockEntry.tryAcquire("node1", 30000);
        
        boolean result = lockEntry.release("node2");
        
        assertFalse(result);
        assertTrue(lockEntry.isHeld());
        assertEquals("node1", lockEntry.getNodeId());
    }

    @Test
    void testReentrantRelease() {
        lockEntry.tryAcquire("node1", 30000);
        lockEntry.tryAcquire("node1", 30000);
        
        lockEntry.release("node1");
        assertTrue(lockEntry.isHeld());
        assertEquals(1, lockEntry.getHoldCount());
        
        lockEntry.release("node1");
        assertFalse(lockEntry.isHeld());
        assertEquals(0, lockEntry.getHoldCount());
    }

    @Test
    void testRenew() {
        lockEntry.tryAcquire("node1", 30000);
        long originalExpireTime = lockEntry.getLeaseExpireTime();
        
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        boolean result = lockEntry.renew("node1", 30000);
        
        assertTrue(result);
        assertTrue(lockEntry.getLeaseExpireTime() > originalExpireTime);
    }

    @Test
    void testRenewByDifferentNodeFails() {
        lockEntry.tryAcquire("node1", 30000);
        
        boolean result = lockEntry.renew("node2", 30000);
        
        assertFalse(result);
    }

    @Test
    void testLeaseExpiry() {
        lockEntry.tryAcquire("node1", 10);
        
        try {
            Thread.sleep(20);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        assertTrue(lockEntry.isLeaseExpired());
    }

    @Test
    void testAcquireAfterExpiry() {
        lockEntry.tryAcquire("node1", 10);
        
        try {
            Thread.sleep(20);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        boolean result = lockEntry.tryAcquire("node2", 30000);
        
        assertTrue(result);
        assertEquals("node2", lockEntry.getNodeId());
    }
}