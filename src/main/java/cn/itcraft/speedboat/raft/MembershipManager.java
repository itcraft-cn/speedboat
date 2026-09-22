package cn.itcraft.speedboat.raft;

import cn.itcraft.speedboat.config.MembershipConfig;
import cn.itcraft.speedboat.rpc.AppendEntriesRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 成员变更管理器（内部协作器，非公开 API）。
 *
 * <p>职责唯一：动态成员的检测（健康/注册中心）、提案（ADD/REMOVE 同构统一）、
 * 应用（状态机 apply 链回调）与故障记录。变更经日志复制实现一致的状态变更
 * （MemberChangeEntry）。</p>
 *
 * <p>遗留 FAQ：成员变更检测历史上因构造期 scheduler 缺失从未真正启动，
 * 现改由门面 start() 后统一武装（见 {@link RaftNodeImpl}）。</p>
 *
 * <p>并发契约：仅限 raft 单线程调用（检测任务由门面投递）。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
final class MembershipManager {

    private static final Logger logger = LoggerFactory.getLogger(MembershipManager.class);

    private final NodeContext ctx;
    private final RaftLogStore logStore;
    private final ReplicationPump pump;

    MembershipManager(NodeContext ctx, RaftLogStore logStore, ReplicationPump pump) {
        this.ctx = ctx;
        this.logStore = logStore;
        this.pump = pump;
    }

    // ==================== 检测 ====================

    /**
     * 检查成员变更（Leader 专用）：
     * <ol>
     *   <li>健康检测 → 不健康节点累计故障 → 达阈值提议移除；</li>
     *   <li>注册中心比对 → 新节点提议添加 / 失联节点提议移除（跳过自己）。</li>
     * </ol>
     */
    void checkMembershipChanges() {
        if (ctx.currentState != NodeState.LEADER) {
            // 只有 Leader 节点执行成员变更检测
            return;
        }

        // 执行健康检测
        List<String> unhealthyPeers = ctx.healthCheckStrategy.checkUnhealthyPeers(ctx.peerIds);
        for (String peerId : unhealthyPeers) {
            recordFailure(peerId);
        }

        // 检查注册中心信息
        List<String> registeredPeers = ctx.registryStrategy.getRegisteredPeers();
        if (registeredPeers != null && !registeredPeers.isEmpty()) {
            // 与当前成员列表比较
            Set<String> currentSet = new HashSet<>(ctx.peerIds);
            currentSet.add(ctx.nodeId); // 包括自己
            Set<String> registeredSet = new HashSet<>(registeredPeers);

            // 发现新节点
            Set<String> newPeers = new HashSet<>(registeredSet);
            newPeers.removeAll(currentSet);
            for (String newPeer : newPeers) {
                proposeAddMember(newPeer);
            }

            // 发现已移除节点（不在注册中心但仍在当前列表中）
            Set<String> removedPeers = new HashSet<>(currentSet);
            removedPeers.removeAll(registeredSet);
            for (String removedPeer : removedPeers) {
                // 跳过自己
                if (!removedPeer.equals(ctx.nodeId)) {
                    proposeRemoveMember(removedPeer);
                }
            }
        }
    }

    /**
     * 记录节点故障：连续失败计数 + 时间窗判定，达阈值提议移除。
     */    private void recordFailure(String peerId) {
        ctx.failureRecords.compute(peerId, (key, record) -> {
            if (record == null) {
                record = new FailureRecord(peerId);
            }
            record.incrementFailures();

            // 检查是否达到故障阈值
            if (record.shouldProposeRemoval(ctx.membershipConfig.getFailureThreshold(), ctx.membershipConfig.getConfirmationNanos())) {
                proposeRemoveMember(peerId);
            }

            return record;
        });
    }

    // ==================== 提案（ADD/REMOVE 同构收敛） ====================

    /**
     * 提出添加成员（对外 API 保留 {@code proposeAddMember} 委派）。
     */
    boolean proposeAddMember(String newPeerId) {
        if (ctx.currentState != NodeState.LEADER) {
            logger.warn("Only leader can propose member additions");
            return false;
        }

        // 验证变更
        boolean valid = ctx.changeValidationStrategy.validateAdd(newPeerId, ctx.peerIds);
        if (!valid) {
            logger.warn("Failed to validate addition of peer {}", newPeerId);
            return false;
        }

        MemberChangeEntry entry = MemberChangeEntry.add(
            logStore.getLastLogIndex() + 1,  // index
            ctx.term.getCurrent(),           // term
            ctx.nodeId,                      // leaderId
            newPeerId,                       // nodeId
            null                             // address
        );

        logStore.appendEntry(entry);
        // 复制到所有节点
        replicateMemberChange(entry);

        logger.info("Leader {} proposed to add member {}", ctx.nodeId, newPeerId);
        return true;
    }

    /**
     * 提出移除成员（对外 API 保留 {@code proposeRemoveMember} 委派）。
     */
    boolean proposeRemoveMember(String peerId) {
        if (ctx.currentState != NodeState.LEADER) {
            logger.warn("Only leader can propose member removals");
            return false;
        }

        // 不能移除自己
        if (peerId.equals(ctx.nodeId)) {
            logger.warn("Cannot remove self from cluster");
            return false;
        }

        // 验证变更
        boolean valid = ctx.changeValidationStrategy.validateRemove(peerId, ctx.peerIds);
        if (!valid) {
            logger.warn("Failed to validate removal of peer {}", peerId);
            return false;
        }

        MemberChangeEntry entry = MemberChangeEntry.remove(
            logStore.getLastLogIndex() + 1,  // index
            ctx.term.getCurrent(),           // term
            ctx.nodeId,                      // leaderId
            peerId                           // nodeId
        );

        logStore.appendEntry(entry);
        // 复制到所有节点
        replicateMemberChange(entry);

        logger.info("Leader {} proposed to remove member {}", ctx.nodeId, peerId);
        return true;
    }

    /**
     * 复制成员变更：被移除节点本身跳过（已离任者不再需要 ingest 该变更）。
     */
    private void replicateMemberChange(MemberChangeEntry entry) {
        if (ctx.transportLayer == null) {
            return;
        }

        for (String peerId : ctx.peerIds) {
            // 如果要移除的节点，跳过
            if (entry.getChangeType() == MemberChangeEntry.ChangeType.REMOVE &&
                entry.getPeerId().equals(peerId)) {
                continue;
            }

            List<LogEntry> entries = Collections.singletonList(entry);
            AppendEntriesRequest request = new AppendEntriesRequest(
                ctx.term.getCurrent(), ctx.nodeId,
                entry.getIndex() - 1, pump.getEntryTerm(entry.getIndex() - 1),
                entries, ctx.commitIndex
            );

            pump.replicateToPeerOverride(peerId, request);
        }
    }

    // ==================== 应用（apply 全序回调） ====================

    /**
     * 处理成员变更（apply 全序内）；Leader 侧附带复制进度初始化/清理。
     */
    void processMemberChange(MemberChangeEntry entry) {
        String peerId = entry.getPeerId();

        if (entry.getChangeType() == MemberChangeEntry.ChangeType.ADD) {
            // 添加新成员
            if (!ctx.peerIds.contains(peerId)) {
                ctx.peerIds.add(peerId);
                logger.info("Node {} added new peer {}", ctx.nodeId, peerId);

                // 如果是 Leader，初始化 nextIndex 和 matchIndex
                if (ctx.currentState == NodeState.LEADER) {
                    ctx.nextIndex.put(peerId, logStore.getLastLogIndex() + 1);
                    ctx.matchIndex.put(peerId, 0L);
                }
            }
        } else if (entry.getChangeType() == MemberChangeEntry.ChangeType.REMOVE) {
            // 移除成员
            if (ctx.peerIds.contains(peerId)) {
                ctx.peerIds.remove(peerId);
                logger.info("Node {} removed peer {}", ctx.nodeId, peerId);

                // 如果是 Leader，清理状态
                if (ctx.currentState == NodeState.LEADER) {
                    ctx.nextIndex.remove(peerId);
                    ctx.matchIndex.remove(peerId);
                }

                // 清理故障记录
                ctx.failureRecords.remove(peerId);
            }
        }
    }
}
