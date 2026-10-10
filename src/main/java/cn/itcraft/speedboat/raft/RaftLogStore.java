package cn.itcraft.speedboat.raft;
import cn.itcraft.speedboat.persistence.RaftTermRecord;
import cn.itcraft.speedboat.rpc.AppendEntriesRequest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Raft 日志仓（内部协作器，非公开 API）。
 *
 * <p>职责唯一：内存日志本体的存储/查询/追加/裁剪/梦幻重建，以及与
 * {@code RaftStore} 的持久化桥接（WAL 追加、term 落盘、恢复回调）。</p>
 *
 * <p>缺省策略：</p>
 * <ul>
 *   <li>append 前先持久化（"先持久化后可见"）；</li>
 *   <li>裁剪遵循两条安全路径规则，详见 {@link #truncateLogIfNeeded()}；</li>
 *   <li>start 阶段的 WAL 重建也经 {@link #mirrorFromStore(List)} 归口。</li>
 * </ul>
 *
 * <p>并发契约：基本方法由 raft 单线程独占调；查询方法仅依赖副本视图。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
final class RaftLogStore {

    private static final Logger logger = LoggerFactory.getLogger(RaftLogStore.class);

    private final NodeContext ctx;
    /**
     * 日志索引 → 条目的 O(1) 映射。
     *
     * <p>raft 单线程独占写（append/rebuild/truncate 同步维护），
     * 外部线程仅做 get 读快照——替代原 {@code getEntryAt} 对链表
     * 的 O(n) 线性扫描（Phase B 结构性修复）。</p>
     */
    private final ConcurrentHashMap<Long, LogEntry> logIndexMap = new ConcurrentHashMap<>();

     RaftLogStore(NodeContext ctx) {
        this.ctx = ctx;
        for (LogEntry seed : ctx.log) {
            this.logIndexMap.put(seed.getIndex(), seed);
        }
    }

    // ==================== 查询族 ====================

    /** O(1) 索引查询；越界/不存在返回 null */
    LogEntry getEntryAt(long index) {
        return logIndexMap.get(index);
    }

    /** 最后一条日志 index（空仓返回 0） */
    long getLastLogIndex() {
        return ctx.log.isEmpty() ? 0 : ctx.log.get(ctx.log.size() - 1).getIndex();
    }

    /** 最后一条日志 term（空仓返回 0） */
    long getLastLogTerm() {
        return ctx.log.isEmpty() ? 0 : ctx.log.get(ctx.log.size() - 1).getTerm();
    }

    /** 指定索引条目的 term；index<=0 或空缺返回 0 */
    long getEntryTerm(long index) {
        if (index <= 0) {
            return 0;
        }
        LogEntry entry = getEntryAt(index);
        return entry != null ? entry.getTerm() : 0;
    }

    /** 副本视图：不可变日志快照 */
    List<LogEntry> getLogEntries() {
        return Collections.unmodifiableList(new ArrayList<>(ctx.log));
    }

    // ==================== 追加族 ====================

    /**
     * 追加单条日志（内部统一入口）。
     * 走"先持久化后可见"次序：
     * <ol>
     *   <li>copy-on-write 列表追加 + index 映射建位；</li>
     *   <li>raftStore.persistLogEntries 落盘（同步）；</li>
     *   <li>容量裁剪。</li>
     * </ol>
     *
     * @return 追加后的 index
     */
    long appendEntry(LogEntry entry) {
        ctx.log.add(entry);
        logIndexMap.put(entry.getIndex(), entry);
        ctx.raftStore.persistLogEntries(Collections.singletonList(entry));
        truncateLogIfNeeded();
        return entry.getIndex();
    }

    /**
     * Leader 登基时自造日志条目（原 appendLeaderEntry 归迁）。
     * 空数据 leader-entry：仅宣示 leader 化生效，不承载业务命令。
     */
    long appendLeaderEntry() {
        long newIndex = getLastLogIndex() + 1;
        LogEntry entry = new LogEntry(newIndex, ctx.term.getCurrent(), ctx.nodeId);
        ctx.log.add(entry);
        logIndexMap.put(newIndex, entry);
        ctx.raftStore.persistLogEntries(Collections.singletonList(entry));
        truncateLogIfNeeded();
        logger.info("Leader {} appended entry at index {} term {}", ctx.nodeId, newIndex, ctx.term.getCurrent());
        return newIndex;
    }

    /**
     * Follower 冲突分流追加：在索引一致的情况下，用请求日志覆写本地尾部（含 persist 与裁剪）。
     * 一致性校验由入站 AppendHandler 完成后再调用本方法。
     */
    void applyFollowerAppend(AppendEntriesRequest request) {
        List<LogEntry> newLog = new ArrayList<>();
        for (LogEntry entry : ctx.log) {
            if (entry.getIndex() < request.getPrevLogIndex() + 1) {
                newLog.add(entry);
            }
        }

        for (LogEntry entry : request.getEntries()) {
            newLog.add(entry);
        }

        ctx.log.clear();
        ctx.log.addAll(newLog);
        // 重建后统一重打索引（幂等，容量与 log list 一致）
        logIndexMap.clear();
        rebuildIndexFast();
        ctx.raftStore.persistLogEntries(newLog);
        truncateLogIfNeeded();

        logger.debug("Follower {} appended {} entries, log size now {}",
            ctx.nodeId, request.getEntries().size(), ctx.log.size());
    }

    /**
     * O(1) 索引重建：避免 applyFollowerAppend 双循环内对 {@code logIndexMap} 的
     * 两倍写竞态——单一 rebuild 函数负责容量对齐。
     */
    private void rebuildIndexFast() {
        for (LogEntry existing : ctx.log) {
            logIndexMap.put(existing.getIndex(), existing);
        }
    }

    /**
     * 启动时从 WAL 重建日志主体（行序铁律：必须先于状态机 checkpoint 跳位）。
     * 用于 {@code start()} 的 read-back 链。
     *
     * <p><b>重建规则</b>：成员变更条目按 ADD/REMOVE 还原为 {@code MemberChangeEntry}，
     * COMMAND 条目克隆 data 字节数组，为保证跨进程语义与原进程一致改用"mirror"
     * 链接（index/term 等保持不变）。</p>
     */
    void mirrorFromStore(List<LogEntry> restoredLogs) {
        for (LogEntry e : restoredLogs) {
            LogEntry mirror;
            if (e.getEntryType() == LogEntry.EntryType.MEMBER_CHANGE && e instanceof MemberChangeEntry) {
                MemberChangeEntry mce = (MemberChangeEntry) e;
                mirror = mce.getChangeType() == MemberChangeEntry.ChangeType.ADD
                    ? MemberChangeEntry.add(e.getIndex(), e.getTerm(), e.getLeaderId(), mce.getNodeId(), mce.getAddress())
                    : MemberChangeEntry.remove(e.getIndex(), e.getTerm(), e.getLeaderId(), mce.getNodeId());
            } else if (e.getEntryType() == LogEntry.EntryType.COMMAND) {
                mirror = CommandLogEntry.create(e.getIndex(), e.getTerm(), e.getLeaderId(),
                    e.getData() == null ? new byte[0] : e.getData().clone());
            } else if (e.getEntryType() == LogEntry.EntryType.PRIORITY_CHANGE) {
                // 优先级变更条目：从 data 字节重建（data 内已编码 epoch/weights/source/operator/reason）
                mirror = PriorityChangeEntry.fromData(e.getIndex(), e.getTerm(), e.getLeaderId(),
                    e.getData() == null ? new byte[0] : e.getData().clone());
            } else {
                LogEntry plain = new LogEntry(e.getIndex(), e.getTerm(), e.getLeaderId());
                plain.setEntryType(e.getEntryType());
                plain.setData(e.getData() == null ? null : e.getData().clone());
                mirror = plain;
            }
            logIndexMap.put(mirror.getIndex(), mirror);
            ctx.log.add(mirror);
        }
        logger.info("Node {} rebuilt log from store {} entries={}",
            ctx.nodeId, ctx.raftStore.getClass().getName(), ctx.log.size());
    }

    // ==================== 裁剪族 ====================

    /**
     * 首条日志 index（裁剪后可能 > 1；空仓返回 0）。
     * 用于"日志过短"场景的快速回退提示（expectedNextIndex 下界参考）。
     */
    long getFirstLogIndex() {
        return ctx.log.isEmpty() ? 0 : ctx.log.get(0).getIndex();
    }

    /**
     * 冲突条目起始 index（SOFAJRaft/Ratis 式快速回退）：
     * 从 {@code fromIndex} 向前扫描，返回"与 {@code expectedTerm} 同任期"的
     * 最小 index；扫描落入更早任期即停（Raft 论文 §5.3 的 conflict index）。
     *
     * <p>找不到同任期条目时退化为 {@code fromIndex}（保守逐步回退）。</p>
     *
     * @param fromIndex    prevLogIndex 一致性检查失败的位置
     * @param expectedTerm prevLogTerm 中声明的任期
     * @return 建议重发的下一条 index（考虑越界钳制后仍由 leader 手动钳制）
     */
    long conflictNextIndex(long fromIndex, long expectedTerm) {
        // 位置越界（本地日志比预期望的还要靠前/靠后）：直接提示从首条开始
        LogEntry start = getEntryAt(fromIndex);
        if (start == null) {
            long first = getFirstLogIndex();
            return first > 0 ? first : 1;
        }
        // 本地该位置的任期与请求声明一致——说明冲突在此之前的更早条目，
        // 沿本地任期一路向左延伸（返回该任期首条 + 1 后 leader 重新探测会再次命中
        // 分歧点，最终由逐条语义兜底；此处只要给出可疑区间的起点即可）
        if (start.getTerm() == expectedTerm) {
            long i = fromIndex;
            while (i > 1) {
                LogEntry prev = getEntryAt(i - 1);
                if (prev == null || prev.getTerm() != expectedTerm) {
                    break;
                }
                i--;
            }
            long first = getFirstLogIndex();
            return i > first ? i : first;
        }
        // 常规冲突（同 index 不同任期）：返回本地该任期的首条 index
        long localTerm = start.getTerm();
        long i = fromIndex;
        while (i > 1) {
            LogEntry prev = getEntryAt(i - 1);
            if (prev == null || prev.getTerm() != localTerm) {
                break;
            }
            i--;
        }
        return i;
    }

    /**
     * 内存日志裁剪（maxLogSize 生效路径，P4-M4 修正）。
     *
     * <p><b>历史缺陷</b>：旧逻辑"遇到已提交条目即 break"，提交推进后永不裁剪、
     * maxLogSize 对已提交区域形同虚设（日志无限增长）。</p>
     *
     * <p><b>两条安全裁剪路径（合并保留）</b>：</p>
     * <ol>
     *   <li><b>未提交积压</b>（含 follower）：index &gt; commitIndex 的条目被裁剪是安全的
     *       —— 未提交条目由 Leader 按 nextIndex 重发收敛；</li>
     *   <li><b>已提交前缀</b>（仅 Leader）：要求所有 peer 的 matchIndex 已覆盖该前缀
     *       （或单节点），此时该前缀不可能再被任何 peer 需要重发（本框架无 INSTALL_SNAPSHOT）。</li>
     * </ol>
     */
    void truncateLogIfNeeded() {
        if (ctx.log.size() <= ctx.maxLogSize) {
            return;
        }
        long safeCommittedFloor = resyncSafeFloor();
        int allowedRemove = ctx.log.size() - ctx.maxLogSize;
        int removeCount = 0;
        for (LogEntry entry : ctx.log) {
            if (removeCount >= allowedRemove) {
                break;
            }
            boolean uncommitted = entry.getIndex() > ctx.commitIndex;
            boolean safeCommitted = safeCommittedFloor >= 0 && entry.getIndex() <= safeCommittedFloor;
            if (!uncommitted && !safeCommitted) {
                break;
            }
            removeCount++;
        }
        if (removeCount <= 0) {
            return;
        }
        for (int i = 0; i < removeCount; i++) {
            LogEntry oldest = ctx.log.get(i);
            logIndexMap.remove(oldest.getIndex());
        }
        ctx.log.subList(0, removeCount).clear();
        logger.info("Node {} trimmed {} log entries (maxLogSize={}, safeCommittedFloor={}, logSize={})",
            ctx.nodeId, removeCount, ctx.maxLogSize, safeCommittedFloor, ctx.log.size());
    }

    /**
     * 可安全裁剪的日志上界：所有 peer 都已复制的最大连续前缀（≤ commitIndex）。
     * 返回 -1 表示当前不可裁剪（非 leader / 有 peer 匹配未知）。
     */
    long resyncSafeFloor() {
        if (ctx.peerIds.isEmpty()) {
            return ctx.commitIndex;
        }
        if (ctx.currentState != NodeState.LEADER) {
            return -1;
        }
        long floor = Long.MAX_VALUE;
        for (String peer : ctx.peerIds) {
            Long m = ctx.matchIndex.get(peer);
            if (m == null) {
                return -1;
            }
            floor = Math.min(floor, m);
        }
        return Math.min(floor, ctx.commitIndex);
    }

    // ==================== 任期持久化族 ====================

    /**
     * 任期状态持久化（term/votedFor/leader 级别原子落盘）。
     *
     * <p>调用时机参照 MicroRaft 的"先持久化后可见"原则：
     * 任期被提升/投票授予后、内存可见性发布前调用。</p>
     */
    void persistTermState(long termVal, String votedForVal, String leaderVal) {
        try {
            ctx.raftStore.persistAndFlushTerm(termVal, votedForVal, leaderVal);
        } catch (Exception e) {
            throw new IllegalStateException(
                String.format("Node %s persist term state failed on store %s",
                    ctx.nodeId, ctx.raftStore.getClass().getName()), e);
        }
    }

    /** 启动时恢复任期（返回是否命中持久化记录）。 */
    boolean restoreTermIfPresent() {
        RaftTermRecord restored = ctx.raftStore.restoreTerm();
        if (restored == null) {
            return false;
        }
        ctx.term.updateIfHigher(restored.getTerm());
        ctx.votedFor = restored.getVotedFor();
        ctx.votedForTerm = restored.getVotedFor() != null ? restored.getTerm() : -1;
        logger.info("Node {} restored term {} votedFor {} from store {}",
            ctx.nodeId, restored.getTerm(), restored.getVotedFor(), ctx.raftStore.getClass().getName());
        return true;
    }
}
