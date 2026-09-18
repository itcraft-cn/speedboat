package cn.itcraft.speedboat.rpc;

/**
 * 锁操作转发响应（Leader → 发起方）。
 *
 * <p><b>仅表示"提案已被 Leader 收录进日志"（ok=true）</b>，
 * 不代表授权成功——真正的判定（GRANTED/DENIED + epoch）在所有副本
 * 状态机 apply 后由发起方从本地结果表读取（全序一致性）。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
public class LockOpResponse extends RpcResponse {

    private boolean ok;

    private long entryIndex;

    public LockOpResponse() {
        super(null);
    }

    public LockOpResponse(String requestId, boolean ok, long entryIndex) {
        super(requestId);
        this.ok = ok;
        this.entryIndex = entryIndex;
    }

    public boolean isOk() {
        return ok;
    }

    public void setOk(boolean ok) {
        this.ok = ok;
    }

    public long getEntryIndex() {
        return entryIndex;
    }

    public void setEntryIndex(long entryIndex) {
        this.entryIndex = entryIndex;
    }
}
