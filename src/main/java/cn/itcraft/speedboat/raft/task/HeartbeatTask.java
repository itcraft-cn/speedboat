package cn.itcraft.speedboat.raft.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Leader 心跳周期任务（对标 MicroRaft task/HeartbeatTask 的"状态感知"设计）。
 *
 * <p>由 {@code RaftNodeExecutor} 以固定节拍调度在 raft 单消费者线程上执行：
 * 仅当节点仍在运行且保持 Leader 角色时才发送心跳；角色降级后由
 * {@code cancelHeartbeat} 取消调度，任务本身无须再加状态锁。</p>
 *
 * <p><b>注意事项：</b>任务必须快速返回（契约禁止阻塞），
 * 实际网络发送在传输层非阻塞完成。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
public class HeartbeatTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(HeartbeatTask.class);

    /** 心跳动作宿主（仅包内接口，避免暴露内部细节） */
    public interface HeartbeatBearer {
        void doSendHeartbeat();
    }

    private final HeartbeatBearer bearer;

    public HeartbeatTask(HeartbeatBearer bearer) {
        this.bearer = bearer;
    }

    @Override
    public void run() {
        try {
            bearer.doSendHeartbeat();
        } catch (Exception e) {
            logger.warn("Heartbeat task failed: {}", e.toString(), e);
        }
    }
}
