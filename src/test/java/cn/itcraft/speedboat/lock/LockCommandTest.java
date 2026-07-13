package cn.itcraft.speedboat.lock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LockCommandTest {

    @Test
    void testLockCommand() {
        LockCommand command = LockCommand.lock("test-lock", "node1");
        
        assertEquals("test-lock", command.getLockName());
        assertEquals("node1", command.getNodeId());
        assertEquals(LockCommand.CommandType.LOCK, command.getCommandType());
        assertTrue(command.getTimestamp() > 0);
    }

    @Test
    void testUnlockCommand() {
        LockCommand command = LockCommand.unlock("test-lock", "node1");
        
        assertEquals("test-lock", command.getLockName());
        assertEquals("node1", command.getNodeId());
        assertEquals(LockCommand.CommandType.UNLOCK, command.getCommandType());
    }

    @Test
    void testRenewCommand() {
        LockCommand command = LockCommand.renew("test-lock", "node1");
        
        assertEquals("test-lock", command.getLockName());
        assertEquals("node1", command.getNodeId());
        assertEquals(LockCommand.CommandType.RENEW, command.getCommandType());
    }

    @Test
    void testSetters() {
        LockCommand command = new LockCommand();
        
        command.setLockName("test-lock");
        command.setNodeId("node1");
        command.setCommandType(LockCommand.CommandType.LOCK);
        command.setTimestamp(12345L);
        
        assertEquals("test-lock", command.getLockName());
        assertEquals("node1", command.getNodeId());
        assertEquals(LockCommand.CommandType.LOCK, command.getCommandType());
        assertEquals(12345L, command.getTimestamp());
    }

    @Test
    void testToString() {
        LockCommand command = LockCommand.lock("test-lock", "node1");
        
        String str = command.toString();
        
        assertTrue(str.contains("test-lock"));
        assertTrue(str.contains("node1"));
        assertTrue(str.contains("LOCK"));
    }
}