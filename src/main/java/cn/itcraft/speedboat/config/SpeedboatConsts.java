package cn.itcraft.speedboat.config;

/**
 * Speedboat 常量定义类
 * 
 * <p>定义 Raft 分布式锁控系统所需的核心常量，包括：</p>
 * <ul>
 *   <li>选举超时时间参数</li>
 *   <li>心跳间隔参数</li>
 *   <li>序列化相关常量</li>
 *   <li>端口配置</li>
 * </ul>
 *
 * @author speedboat
 * @since 1.0.0
 */
public final class SpeedboatConsts {

    private SpeedboatConsts() {
    }

    public static final int MIN_ELECTION_TIMEOUT_MS = 150;
    
    public static final int MAX_ELECTION_TIMEOUT_MS = 300;
    
    public static final int HEARTBEAT_INTERVAL_MS = 50;
    
    public static final int GLOBAL_MIN_ELECTION_TIMEOUT_MS = 500;
    
    public static final int GLOBAL_MAX_ELECTION_TIMEOUT_MS = 1000;
    
    public static final int GLOBAL_HEARTBEAT_INTERVAL_MS = 200;
    
    public static final int DEFAULT_SERIALIZATION_TYPE = 2;
    
    public static final int SERIALIZATION_HEADER_LEN = 9;
    
    public static final int CRC32_LEN = 4;
    
    public static final int TYPE_LEN = 1;
    
    public static final int DEFAULT_PORT = 22222;
    
    public static final int GLOBAL_PORT = 33333;
    
    public static final int SHUTDOWN_TIMEOUT_SECONDS = 1;

    /**
     * 内存日志保留条数上限（defect-20260918-01 校正：10 → 4096）。
     *
     * <p><b>根源与语义（必须懂再改）：</b>日志是重启副本重建锁表/判定状态的唯一
     * 依据（快照/持久化上线前）。上限过小时历史被截断 → 重启副本 apply 到空态
     * → epoch/fencing 等判定跨副本分叉。按锁租约续期节奏（lease/2 条/锁/租期）
     * 估算：4096 条足以覆盖典型"重锁名目 × 30s 租约 × 数小时"场景；
     * 更长期的运行仍必须依赖快照（P4 C）/持久化（P4 D）。</p>
     */
    public static final int DEFAULT_MAX_LOG_SIZE = 4096;

    /**
     * 分布式锁默认租约超时（毫秒）。
     *
     * <p>唯一权威定义：LockStateMachine 与 DistributedLockImpl 的续期间隔
     * 均以此为准（间隔 = 租期 / 2），禁止在 lock 包内另定义副本——
     * 历史上两处各自硬编码 30000，修改时极易遗漏造成续期与租期不一致。</p>
     */
    public static final long DEFAULT_LEASE_TIMEOUT_MS = 30000L;
}