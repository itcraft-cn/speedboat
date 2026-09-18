package cn.itcraft.speedboat.lock;

import java.io.Serializable;
import java.util.UUID;

/**
 * 锁命令（Raft 日志中的锁操作提案载荷）。
 *
 * <p><b>多持有者语义（2026-09-18 命名锁设计）：</b></p>
 * <ul>
 *   <li>{@code nodeId} = 真实申请者身份，与"谁是 Leader"无关——
 *       Leader 只是命令全序化的仲裁者，转发场景下必须原样保留申请者身份；</li>
 *   <li>{@code requestId} 全链路幂等键：转发重试、日志重放均以其去重；</li>
 *   <li>{@code leaseMs} 申请方指定的租约时长（缺省回退 LockStateMachine 默认值）；</li>
 *   <li>{@code grantTimestampMs} = <b>Leader 打点</b>的授权时刻，
 *       随命令复制后全网到期点一致（消除各副本本地打点漂移），
 *       必须在 propose 之前由 Leader 侧设置。</li>
 * </ul>
 *
 * @author speedboat
 * @since 1.0.0
 */
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

    /** 请求幂等标识（UUID）；重试/重放以此去重，避免重入计数被重复累加 */
    private String requestId;

    /** 申请租约时长（毫秒）；0 表示使用状态机默认值（兼容旧命令） */
    private long leaseMs;

    /** Leader 授权打点（毫秒墙钟）；apply 侧以 timestamp + leaseMs 计算全网一致到期点 */
    private long grantTimestampMs;

    public LockCommand() {
    }

    public LockCommand(String lockName, String nodeId, CommandType commandType) {
        this(lockName, nodeId, commandType, UUID.randomUUID().toString(), 0, 0);
    }

    /**
     * 全参构造（产品路径）。
     *
     * @param lockName         锁名（名目）
     * @param nodeId           真实申请者节点 ID
     * @param commandType      命令类型
     * @param requestId        幂等标识，由申请方生成
     * @param leaseMs          租约时长毫秒数
     * @param grantTimestampMs Leader 授权打点（propose 前设置，0=未打点）
     */
    public LockCommand(String lockName, String nodeId, CommandType commandType,
                       String requestId, long leaseMs, long grantTimestampMs) {
        this.lockName = lockName;
        this.nodeId = nodeId;
        this.commandType = commandType;
        this.timestamp = grantTimestampMs > 0 ? grantTimestampMs : System.currentTimeMillis();
        this.requestId = requestId;
        this.leaseMs = leaseMs;
        this.grantTimestampMs = grantTimestampMs;
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

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public long getLeaseMs() {
        return leaseMs;
    }

    public void setLeaseMs(long leaseMs) {
        this.leaseMs = leaseMs;
    }

    public long getGrantTimestampMs() {
        return grantTimestampMs;
    }

    public void setGrantTimestampMs(long grantTimestampMs) {
        this.grantTimestampMs = grantTimestampMs;
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
            ", requestId='" + requestId + '\'' +
            ", leaseMs=" + leaseMs +
            '}';
    }
}
