package cn.itcraft.speedboat.lock;

import cn.itcraft.speedboat.raft.CommandLogEntry;
import cn.itcraft.speedboat.raft.LogEntry;
import cn.itcraft.speedboat.serialize.ProtostuffSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LockStateMachineTest {

    private LockStateMachine stateMachine;
    private ProtostuffSerializer serializer;

    @BeforeEach
    void setUp() {
        stateMachine = new LockStateMachine();
        serializer = new ProtostuffSerializer();
    }

    @Test
    void testApplyLockCommand() {
        LockCommand command = LockCommand.lock("test-lock", "node1");
        byte[] data = serializer.serialize(command);
        
        LogEntry entry = CommandLogEntry.create(1, 1, "node1", data);
        stateMachine.apply(entry);
        
        assertTrue(stateMachine.isLockHeldBy("test-lock", "node1"));
    }

    @Test
    void testApplyUnlockCommand() {
        LockCommand lockCmd = LockCommand.lock("test-lock", "node1");
        byte[] lockData = serializer.serialize(lockCmd);
        LogEntry lockEntry = CommandLogEntry.create(1, 1, "node1", lockData);
        stateMachine.apply(lockEntry);
        
        LockCommand unlockCmd = LockCommand.unlock("test-lock", "node1");
        byte[] unlockData = serializer.serialize(unlockCmd);
        LogEntry unlockEntry = CommandLogEntry.create(2, 1, "node1", unlockData);
        stateMachine.apply(unlockEntry);
        
        assertFalse(stateMachine.isLockHeldBy("test-lock", "node1"));
    }

    @Test
    void testApplyRenewCommand() {
        LockCommand lockCmd = LockCommand.lock("test-lock", "node1");
        byte[] lockData = serializer.serialize(lockCmd);
        LogEntry lockEntry = CommandLogEntry.create(1, 1, "node1", lockData);
        stateMachine.apply(lockEntry);
        
        LockCommand renewCmd = LockCommand.renew("test-lock", "node1");
        byte[] renewData = serializer.serialize(renewCmd);
        LogEntry renewEntry = CommandLogEntry.create(2, 1, "node1", renewData);
        stateMachine.apply(renewEntry);
        
        assertTrue(stateMachine.isLockHeldBy("test-lock", "node1"));
    }

    @Test
    void testApplyNonCommandEntry() {
        LogEntry entry = new LogEntry(1, 1, "node1");
        
        stateMachine.apply(entry);
        
        assertEquals(0, stateMachine.getLockCount());
    }

    @Test
    void testApplyNullEntry() {
        stateMachine.apply(null);
        
        assertEquals(0, stateMachine.getLockCount());
    }

    @Test
    void testApplyEntryWithNullData() {
        LogEntry entry = CommandLogEntry.create(1, 1, "node1", null);
        
        stateMachine.apply(entry);
        
        assertEquals(0, stateMachine.getLockCount());
    }

    @Test
    void testIsLockAvailable() {
        assertTrue(stateMachine.isLockAvailable("test-lock", "node1"));
        
        LockCommand command = LockCommand.lock("test-lock", "node1");
        byte[] data = serializer.serialize(command);
        LogEntry entry = CommandLogEntry.create(1, 1, "node1", data);
        stateMachine.apply(entry);
        
        assertFalse(stateMachine.isLockAvailable("test-lock", "node2"));
        assertTrue(stateMachine.isLockAvailable("test-lock", "node1"));
    }

    @Test
    void testGetLockEntry() {
        assertNull(stateMachine.getLockEntry("test-lock"));
        
        LockCommand command = LockCommand.lock("test-lock", "node1");
        byte[] data = serializer.serialize(command);
        LogEntry entry = CommandLogEntry.create(1, 1, "node1", data);
        stateMachine.apply(entry);
        
        LockEntry lockEntry = stateMachine.getLockEntry("test-lock");
        assertNotNull(lockEntry);
        assertEquals("node1", lockEntry.getNodeId());
    }

    @Test
    void testGetLastAppliedIndex() {
        assertEquals(0, stateMachine.getLastAppliedIndex());
        
        LockCommand command = LockCommand.lock("test-lock", "node1");
        byte[] data = serializer.serialize(command);
        LogEntry entry = CommandLogEntry.create(5, 1, "node1", data);
        stateMachine.apply(entry);
        
        assertEquals(5, stateMachine.getLastAppliedIndex());
    }

    @Test
    void testGetLockCount() {
        assertEquals(0, stateMachine.getLockCount());
        
        LockCommand cmd1 = LockCommand.lock("lock1", "node1");
        byte[] data1 = serializer.serialize(cmd1);
        LogEntry entry1 = CommandLogEntry.create(1, 1, "node1", data1);
        stateMachine.apply(entry1);
        
        assertEquals(1, stateMachine.getLockCount());
        
        LockCommand cmd2 = LockCommand.lock("lock2", "node1");
        byte[] data2 = serializer.serialize(cmd2);
        LogEntry entry2 = CommandLogEntry.create(2, 1, "node1", data2);
        stateMachine.apply(entry2);
        
        assertEquals(2, stateMachine.getLockCount());
    }
}