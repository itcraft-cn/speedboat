package cn.itcraft.speedboat.lock;

import java.io.Serializable;

public class LockCommand implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum CommandType {
        LOCK,
        UNLOCK,
        RENEW
    }

    private String lockName;
    private String nodeId;
    private CommandType commandType;
    private long timestamp;

    public LockCommand() {
    }

    public LockCommand(String lockName, String nodeId, CommandType commandType) {
        this.lockName = lockName;
        this.nodeId = nodeId;
        this.commandType = commandType;
        this.timestamp = System.currentTimeMillis();
    }

    public String getLockName() {
        return lockName;
    }

    public void setLockName(String lockName) {
        this.lockName = lockName;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public CommandType getCommandType() {
        return commandType;
    }

    public void setCommandType(CommandType commandType) {
        this.commandType = commandType;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public static LockCommand lock(String lockName, String nodeId) {
        return new LockCommand(lockName, nodeId, CommandType.LOCK);
    }

    public static LockCommand unlock(String lockName, String nodeId) {
        return new LockCommand(lockName, nodeId, CommandType.UNLOCK);
    }

    public static LockCommand renew(String lockName, String nodeId) {
        return new LockCommand(lockName, nodeId, CommandType.RENEW);
    }

    @Override
    public String toString() {
        return "LockCommand{" +
            "lockName='" + lockName + '\'' +
            ", nodeId='" + nodeId + '\'' +
            ", commandType=" + commandType +
            ", timestamp=" + timestamp +
            '}';
    }
}