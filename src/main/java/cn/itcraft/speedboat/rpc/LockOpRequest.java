package cn.itcraft.speedboat.rpc;

/**
 * 锁操作转发请求（follower → Leader）。
 *
 * <p>非 Leader 成员申请/续期/释放锁时，把序列化的 {@code LockCommand}
 * 经本消息转发给 Leader：Leader 校验身份后本地 propose 并回执日志索引；
 * 判定结果由申请节点从自身状态机 apply 后的结果表读取（见 LockOpResult）。</p>
 *
 * <p>序列化注意：重载 RuntimeSchema 需要 POJO（无参构造 + getter/setter）。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
public class LockOpRequest extends RpcRequest {

    private String routingNodeId;

    private byte[] command;

    public LockOpRequest() {
        super();
    }

    public LockOpRequest(byte[] command) {
        super();
        this.command = command;
    }

    /** 消息途经的可选路由信息（排障用） */
    public String getRoutingNodeId() {
        return routingNodeId;
    }

    public void setRoutingNodeId(String routingNodeId) {
        this.routingNodeId = routingNodeId;
    }

    public byte[] getCommand() {
        return command;
    }

    public void setCommand(byte[] command) {
        this.command = command;
    }
}
