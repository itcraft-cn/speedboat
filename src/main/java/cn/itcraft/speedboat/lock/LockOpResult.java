package cn.itcraft.speedboat.lock;

/**
 * 锁操作判定结果（单次 LOCK/RENEW/UNLOCK 命令在状态机 apply 后的权威结论）。
 *
 * <p>互斥的最终裁判在状态机 apply（日志全序），转发层与调用方本地预检
 * 仅有优化意义；本结果是所有节点的确定性共识产物。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
public class LockOpResult {

    /** 判定通过时间（毫秒墙钟，用于结果表过期清理） */
    private final long stampMillis;
    /** 是否成功（LOCK 成功=持有权授予；RENEW 成功=租约延长；UNLOCK 成功=释放） */
    private final boolean success;
    /** 成功时返回该山目的持有 epoch（fencing token，所有权变更 +1）；失败时为 -1 */
    private final long epoch;
    /** 失败原因（DENIED_HELD / DENIED_NOT_HOLDER），成功时为 null */
    private final String errCode;

    private LockOpResult(long stampMillis, boolean success, long epoch, String errCode) {
        this.stampMillis = stampMillis;
        this.success = success;
        this.epoch = success ? epoch : -1L;
        this.errCode = errCode;
    }

    public static LockOpResult granted(long epoch) {
        return new LockOpResult(System.currentTimeMillis(), true, epoch, null);
    }

    public static LockOpResult denied(String errCode) {
        return new LockOpResult(System.currentTimeMillis(), false, -1, errCode);
    }

    public long getStampMillis() { return stampMillis; }
    public boolean isSuccess() { return success; }
    public long getEpoch() { return epoch; }
    public String getErrCode() { return errCode; }

    @Override
    public String toString() {
        return "LockOpResult{" + "stamp=" + stampMillis + ", success=" + success
            + ", epoch=" + epoch + ", errCode='" + errCode + "'}";
    }
}
