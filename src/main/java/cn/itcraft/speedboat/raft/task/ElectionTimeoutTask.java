package cn.itcraft.speedboat.raft.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 选举超时任务（对标 MicroRaft task/LeaderElectionTimeoutTask 的形态统一）。
 *
 * <p>执行于 raft 单消费者线程；判定逻辑（心跳真超时 → 发起选举 /
 * 未超时 → 重新武装下一次检测）由宿主 {@code RaftNodeImpl} 以
 * bảo内聚实现，任务仅做"状态感知 + 异常兜底"外壳。</p>
 *
 * <p><b>注意事项：</b>任务必须快速返回；其内部的判定与自续期
 * 均在 raft 单线程上串行，无需任何锁。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
public class ElectionTimeoutTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ElectionTimeoutTask.class);

    /** 选举超时判定动作宿主（单抽象方法，容忍 lambda 直连） */
    public interface ElectionTimeoutChecker {
        void doRunElectionTimeoutCheck();
    }

    private final ElectionTimeoutChecker checker;

    public ElectionTimeoutTask(ElectionTimeoutChecker checker) {
        this.checker = checker;
    }

    @Override
    public void run() {
        try {
            checker.doRunElectionTimeoutCheck();
        } catch (Exception e) {
            logger.warn("ElectionTimeout task failed: {}", e.toString(), e);
        }
    }
}
