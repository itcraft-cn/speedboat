package cn.itcraft.speedboat.raft.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Leader 成员变更检测周期任务（健康检测 + 注册中心 diff → propose）。
 *
 * <p>对标 MicroRaft "状态感知任务" 设计：仅在 Leader 角色下生效，
 * 非 Leader 直接返回，避免follower侧重复检测（与 MembershipCoordinator
 * 双跑的收敛点在宿主内部判断）。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
public class MemberCheckTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(MemberCheckTask.class);

    public interface MemberCheckBearer {
        void doCheckMembershipChanges();
    }

    private final MemberCheckBearer bearer;

    public MemberCheckTask(MemberCheckBearer bearer) {
        this.bearer = bearer;
    }

    @Override
    public void run() {
        try {
            bearer.doCheckMembershipChanges();
        } catch (Exception e) {
            logger.warn("MemberCheck task failed: {}", e.toString(), e);
        }
    }
}
