package cn.itcraft.speedboat.raft;
import cn.itcraft.speedboat.raft.report.RaftNodeReport;
import java.util.Arrays;
import java.util.EnumMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 角色状态机（内部协作器，非公开 API）。
 *
 * <p>职责唯一：FOLLOWER/CANDIDATE/LEADER 的合法迁移、迁移时的入口副作用
 * （清票/term 自增、心跳武装/解除），以及角色只读观测。非法迁移抛
 * {@link IllegalStateException}（RaftNode 的 Barrier 式语义保持不变）。</p>
 *
 * <p>副作用经 {@link RoleHooks} 回注，避免状态机直接持有调度组件——
 * 心跳节拍/选举超时武装仍由门面统一管理。</p>
 *
 * <p>并发契约：全部方法仅限 raft 单线程调用。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
final class RoleMachine {

    private static final Logger logger = LoggerFactory.getLogger(RoleMachine.class);

    /** 角色迁移副作用宿主（由门面/组件装配：心跳武装、选举超时重置、日志/复制挂载） */
    interface RoleHooks {
        void onEnterLeader();
        void onEnterFollower();
        void onEnterCandidate();
    }

    private final NodeContext ctx;
    private final RoleHooks hooks;
    /** 角色行为表（上策状态模式：FOLLOWER/CANDIDATE/LEADER 三态类分发） */
    private final EnumMap<NodeState, RaftRole> roles;
    /** 角色生命周期挂点（心跳武装/撤销、选举超时重置；由门面提供） */
    private RoleLifecycle lifecycle;
    /** 状态报告双通道发布（构造后由门面注入，避免循环依赖） */
    private ReportPublisher reporter;

    RoleMachine(NodeContext ctx, RoleHooks hooks) {
        this.ctx = ctx;
        this.hooks = hooks;
        this.roles = new EnumMap<>(NodeState.class);
        for (RaftRole role : Arrays.asList(
            new FollowerRole(ctx), new CandidateRole(ctx), new LeaderRole())) {
            this.roles.put(role.state(), role);
        }
    }

    /** 注入生命周期挂点（门面布线） */
    void bind(RoleLifecycle lifecycle) {
        this.lifecycle = lifecycle;
    }

    /** 注入状态报告发布器（构造后由门面注入，避免循环依赖） */
    void bind(ReportPublisher reporter) {
        this.reporter = reporter;
    }

    /**
     * raft 线程内的角色迁移：
     * <ol>
     *   <li>合法性检查（非法迁移抛 {@code IllegalStateException}）；</li>
     *   <li>CANDIDATE 入口副作用：清空投票 + 自投票 + term 自增；</li>
     *   <li>LEADER 入口副作用：武装心跳（经 hooks）；</li>
     *   <li>FOLLOWER 入口副作用：取消心跳 + 重置选举超时（经 hooks）；</li>
     *   <li>发布角色变更报告（事件通道）。</li>
     * </ol>
     */
    void doTransitionTo(NodeState newState) {
        if (!ctx.currentState.canTransitionTo(newState)) {
            throw new IllegalStateException(
                String.format("Invalid state transition from %s to %s on node %s",
                    ctx.currentState, newState, ctx.nodeId));
        }

        NodeState oldState = ctx.currentState;
        ctx.currentState = newState;

        if (oldState == NodeState.LEADER && newState == NodeState.FOLLOWER) {
            hooks.onEnterFollower();
        }

        // 角色行为分发（上策状态模式）：分发到明确语义实现（FollowerRole/CandidateRole/LeaderRole）
        RaftRole role = roles.get(newState);
        if (role != null && lifecycle != null) {
            role.enter(lifecycle);
        } else if (hooks != null) {
            // 兼容兜底：未挂 lifecycle 时仍走 hooks（单测场景）
            if (newState == NodeState.CANDIDATE) {
                hooks.onEnterCandidate();
            } else if (newState == NodeState.LEADER) {
                hooks.onEnterLeader();
            } else if (newState == NodeState.FOLLOWER) {
                hooks.onEnterFollower();
            }
        }

        publishReport(RaftNodeReport.ReportReason.ROLE_CHANGE);
        logger.info("Node {} transitioned from {} to {}", ctx.nodeId, oldState, newState);
    }

    /** 是否当前 Leader（进程内观点读，与 {@code isMain} 同义） */
    boolean isLeader() {
        return ctx.currentState == NodeState.LEADER;
    }

    /** 观测只读方法：等价 {@code isLeader()}（API 命名语义保持） */
    boolean isMain() {
        return isLeader();
    }

    /**
     * 角色变更事件触发的状态快照发布（逻辑归 {@link ReportPublisher}，
     * 此处仅保留事件触发点，避免状态机依赖观测组件细节）。
     */
    private void publishReport(RaftNodeReport.ReportReason reason) {
        if (reporter != null) {
            reporter.publish(reason);
        }
    }
}
