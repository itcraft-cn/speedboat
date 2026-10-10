package cn.itcraft.speedboat.raft;
import cn.itcraft.speedboat.raft.task.ElectionTimeoutTask;
import cn.itcraft.speedboat.rpc.PreVoteRequest;
import cn.itcraft.speedboat.rpc.PreVoteResponse;
import cn.itcraft.speedboat.rpc.RequestVoteRequest;
import cn.itcraft.speedboat.rpc.RequestVoteResponse;
import cn.itcraft.speedboat.strategy.consistency.ConsistencyPolicy;
import cn.itcraft.speedboat.strategy.leadership.LeadershipPolicy;
import cn.itcraft.speedboat.strategy.voteweight.DatacenterPriorityTable;
import cn.itcraft.speedboat.strategy.voteweight.PriorityCodec;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 选举协调器（内部协作器，非公开 API）。
 *
 * <p>职责唯一：选举全生命周期——正式选举（RequestVote）、预投票探测（PreVote）、
 * 投票受理（stickiness/term/安全校验）、选举超时判定，以及登基编排。</p>
 *
 * <p>协作规则：</p>
 * <ul>
 *   <li>角色迁移经 {@link RoleMachine}（单一写者），不在本类直接改 ctx.currentState；</li>
 *   <li>出站复制触发经 {@code Runnable sendAppendFlows}（由 ReplicationPump 提供，避免环）；</li>
 *   <li>check-quorum 独立为 {@code HeartbeatWatchdog}，由心跳节拍调用。</li>
 * </ul>
 *
 * <p>并发契约：全部方法仅限 raft 单线程调用。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
final class ElectionCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(ElectionCoordinator.class);

    private final NodeContext ctx;
    private final RoleMachine roles;
    private final QuorumCalculator quorum;
    private final RaftLogStore logStore;
    /** Leader 登基后立即广播一轮（由 ReplicationPump 提供） */
    private final Runnable sendAppendFlows;

    ElectionCoordinator(NodeContext ctx, RoleMachine roles, QuorumCalculator quorum,
                        RaftLogStore logStore, Runnable sendAppendFlows) {
        this.ctx = ctx;
        this.roles = roles;
        this.quorum = quorum;
        this.logStore = logStore;
        this.sendAppendFlows = sendAppendFlows;
    }

    // ==================== 正式选举 ====================

    /**
     * 发起正式选举（raft 单线程内）。
     * 自身权重已达标则立即登基；否则广播 RequestVote 并武装超时自选循环。
     */
    void doStartElection() {
        if (ctx.currentState != NodeState.FOLLOWER && ctx.currentState != NodeState.CANDIDATE) {
            return;
        }

        // 跨机房父组：无代表席位的进程不得进入选举（把关在选举入口，而非只在定时器）。
        //
        // 父组是**全网互联**的，本机房的非代表进程也在 peer 列表中、仅作 learner。
        // 它们虽由 runElectionTimeout 的席位门控拦住不会自发竞选，但 RaftNode.startElection()
        // 是公开入口；按机房聚合分母后，主机房 learner 的自身权重(self=2)已足以单独达到
        // required(2)，一旦被调用即成父组 Leader。后果不是双主而是**静默无主**：
        // 真实子组代表因 isParentLeader=false 而 isMain=false，
        // 该 learner 又因 isIntraLeader=false 而 isMain=false → 全网 isMain 恒为 0。
        //
        // 缺省 seatHeld=true（NodeContext 初值），故子组与单机房路径行为完全不变。
        if (!ctx.seatHeld) {
            logger.info("Node {} holds no representative seat, refusing to start election", ctx.nodeId);
            return;
        }

        // AP 降级判定（follower 侧）：备机房代表在父组是 follower，唯一信号是"多久没收到
        // 父组 Leader 心跳"——而父组 Leader 恒在对侧机房，故 lastHeartbeatNanos 即对侧存活度。
        // CP 恒 no-op，此后的 selfWeight/requiredWeight 与引入前逐字节等价。
        // 必须在计算权重之前评估：降级会收缩分母、使 required 下降，本机房方能单方成主。
        ConsistencyPolicy policy = ctx.consistencyPolicy;
        if (policy != null && policy.allowDegradedTakeover()) {
            long now = System.nanoTime();
            policy.evaluateDegraded(ctx.datacenter, quorum.oppositeDatacenter(),
                ctx.seatHeld, ctx.lastHeartbeatNanos, now);
        }

        roles.doTransitionTo(NodeState.CANDIDATE);
        logStore.persistTermState(ctx.term.getCurrent(), ctx.votedFor, ctx.leaderId);
        ctx.votesReceived.put(ctx.nodeId, true);

        // 自身权重（原 calculateTotalVoteWeight 语义）：base 1 + 策略附加权重；
        // 若达到法定人数（如单节点集群/权重独占多数）则免广播直接登基。
        int currentWeight = quorum.selfWeight(ctx.term.getCurrent());
        int requiredWeight = quorum.requiredWeight();

        logger.info("Node {} starting election for term {}, current weight: {}, required weight: {}",
            ctx.nodeId, ctx.term.getCurrent(), currentWeight, requiredWeight);

        if (currentWeight >= requiredWeight) {
            doBecomeLeader();
            return;
        }

        requestVotesFromPeers(requiredWeight);
    }

    /**
     * 广播 RequestVote；响应回收：
     * <ul>
     *   <li>Granted → 累积投票并判权（达标即登基）；</li>
     *   <li>更高 term → 对齐并降级 FOLLOWER；</li>
     *   <li>超时兜底 → 仍是 CANDIDATE 就再选一轮。</li>
     * </ul>
     */
    private void requestVotesFromPeers(int requiredWeight) {
        if (ctx.transportLayer == null || ctx.peerIds.isEmpty()) {
            checkElectionResult(quorum.selfWeight(ctx.term.getCurrent()), requiredWeight);
            return;
        }

        for (String peerId : ctx.peerIds) {
            // 自报机房：权重策略（如机房优先级权重）依赖此信息判断候选者机房归属
            RequestVoteRequest request = new RequestVoteRequest(
                ctx.term.getCurrent(), ctx.nodeId, quorum.selfWeight(ctx.term.getCurrent()), ctx.datacenter
            );
            // 阶段五：投票请求同样携带优先级快照，投票方可顺带收敛到更高 epoch
            attachPrioritySnapshot(request);

            ctx.transportLayer.sendRequestVote(peerId, request)
                .thenAccept(response -> ctx.executor.execute(() -> {
                    if (response.isVoteGranted() && ctx.currentState == NodeState.CANDIDATE) {
                        ctx.votesReceived.put(peerId, true);
                        checkElectionResult(quorum.receivedWeight(ctx.votesReceived), requiredWeight);
                    } else if (response.getTerm() > ctx.term.getCurrent()) {
                        ctx.term.updateIfHigher(response.getTerm());
                        roles.doTransitionTo(NodeState.FOLLOWER);
                    }
                }));
        }

        ctx.executor.schedule(() -> {
            if (ctx.currentState == NodeState.CANDIDATE) {
                logger.info("Node {} election timeout, starting new election", ctx.nodeId);
                doStartElection();
            }
        }, ctx.electionTimeout.getNext());
    }

    private void checkElectionResult(int currentWeight, int requiredWeight) {
        if (ctx.currentState != NodeState.CANDIDATE) {
            return;
        }

        logger.info("Node {} has weight {}, needs {}", ctx.nodeId, currentWeight, requiredWeight);

        if (currentWeight >= requiredWeight) {
            doBecomeLeader();
        }
    }

    // ==================== 登基 ====================

    /** 登基编排（raft 线程内）：角色迁移 + leader-entry + 进度初始化 + 首轮复制。 */
    void doBecomeLeader() {
        if (ctx.currentState != NodeState.CANDIDATE) {
            return;
        }

        // 席位门控（与 doStartElection 同一条不变式，此处为第二道入口）：
        // RaftNode.becomeLeader() 也是公开 API，学习者不应能被直接推上父组 Leader。
        if (!ctx.seatHeld) {
            logger.info("Node {} holds no representative seat, refusing to become leader", ctx.nodeId);
            return;
        }

        roles.doTransitionTo(NodeState.LEADER);
        ctx.leaderId = ctx.nodeId;

        logStore.appendLeaderEntry();

        // Become leader时，设置matchIndex：自己的matchIndex为logSize，peer的matchIndex为0
        long logSize = logStore.getLastLogIndex();
        long now = System.nanoTime();
        for (String peerId : ctx.peerIds) {
            ctx.nextIndex.put(peerId, logSize + 1);
            ctx.matchIndex.put(peerId, 0L);
            ctx.lastResponseNanos.put(peerId, now);
        }
        ctx.matchIndex.put(ctx.nodeId, logSize);

        // Become leader时，立即提交自己提出的entry（Raft协议要求）
        ctx.commitIndex = logSize;
        applyFlow.applyCommittedEntries();

        sendAppendFlows.run();
        logger.info("Node {} became leader for term {}, commitIndex set to {}", ctx.nodeId, ctx.term.getCurrent(), ctx.commitIndex);
    }

    /**
     * 出站 RequestVote 附带优先级快照（阶段五传播载体）。
     *
     * <p>无优先级表（子组/单机房）或 epoch 为 0（从未变更）时不附带，
     * 旧版本节点收到 null/0 字段据此忽略，协议向后兼容。</p>
     */
    private void attachPrioritySnapshot(RequestVoteRequest request) {
        DatacenterPriorityTable table = ctx.priorityTable;
        if (table == null) {
            return;
        }
        DatacenterPriorityTable.Snapshot snapshot = table.current();
        if (snapshot.epoch() <= 0) {
            return;
        }
        request.setPrioritySnapshot(snapshot.epoch(),
            PriorityCodec.encode(snapshot.weights()));
    }

    /**
     * 入站消息携带的优先级快照收敛（阶段五；与是否授票无关）。
     * 委派到 {@link NodeContext#adoptPrioritySnapshot}（与 AppendEntries 路径共用同一实现）。
     *
     * @param remoteEpoch   远端携带的 epoch
     * @param remoteWeights 远端携带的权重表编码（"dc=w,dc=w"）；可为 null
     */
    void adoptPrioritySnapshot(long remoteEpoch, String remoteWeights) {
        ctx.adoptPrioritySnapshot(remoteEpoch, remoteWeights);
    }

    // ==================== 预投票 ====================

    /**
     * 预投票探测（Phase C，对标 MicroRaft pre-vote）。
     *
     * <p>不递增真实任期地询问多数派"若我发起 term+1 正式选举你愿意投票吗"；
     * 探测超时仍未转正式选举则直接进入正式选举（自复位语义），
     * 保证活性不回退：链路畅通时预票一次到达即选主，失败最多多等一轮超时。</p>
     */
    void doStartPreVote() {
        if (!ctx.running || ctx.transportLayer == null) {
            return;
        }
        if (ctx.currentState != NodeState.FOLLOWER) {
            return;
        }
        // 已有探测在途则不重复发起（防任务重入）
        if (ctx.prevoting) {
            return;
        }
        // 自适应预投票：本集群尚未产生任何任期（term==0 且无 leader）时跳过探测，
        // 直接进入正式选举——保证首主选择在单轮选举窗口内完成（与基线兼容）
        if (ctx.term.getCurrent() == 0 && ctx.leaderId == null) {
            doStartElection();
            return;
        }

        ctx.prevoting = true;
        ctx.prevotesReceived.clear();
        ctx.prevotesReceived.add(ctx.nodeId);
        long probeTerm = ctx.term.getCurrent() + 1;
        logger.info("Node {} starting pre-vote for term {}", ctx.nodeId, probeTerm);

        if (ctx.peerIds.isEmpty()) {
            ctx.prevoting = false;
            doStartElection();
            return;
        }

        long lastLogIndex = logStore.getLastLogIndex();
        long lastLogTerm = logStore.getLastLogTerm();
        final long capturedProbeTerm = probeTerm;
        for (String peerId : ctx.peerIds) {
            // 自报机房：与 RequestVote 保持一致，供对端权重策略判断候选者机房归属
            PreVoteRequest request = new PreVoteRequest(probeTerm, ctx.nodeId, lastLogIndex, lastLogTerm, ctx.datacenter);
            ctx.transportLayer.sendPreVote(peerId, request)
                .thenAccept(response -> ctx.executor.execute(
                    () -> doHandlePreVoteResponse(peerId, response, capturedProbeTerm)));
        }

        // 探测超时：仍未转正式选举（多数派拒绝/失联）→ 兜底进入正式选举保证活性
        ctx.executor.schedule(() -> {
            if (ctx.prevoting) {
                ctx.prevoting = false;
                logger.info("Node {} pre-vote timeout, fallback to real election", ctx.nodeId);
                doStartElection();
            }
        }, ctx.electionTimeout.getNext());
    }

    void doHandlePreVoteResponse(String peerId, PreVoteResponse response, long probeTerm) {
        if (!ctx.prevoting) {
            return;
        }
        if (response.isVoteGranted()) {
            ctx.prevotesReceived.add(peerId);
            int grantedWeight = quorum.grantedWeight(ctx.prevotesReceived, probeTerm);
            if (grantedWeight >= quorum.requiredWeight()) {
                ctx.prevoting = false;
                logger.info("Node {} pre-vote quorum reached ({}>={}), promoting to real election",
                    ctx.nodeId, grantedWeight, quorum.requiredWeight());
                doStartElection();
            }
            return;
        }

        // 明确拒绝且应答任期高于本地任期：仅中止本轮探测并降级，
        // 不做 term 爬升（拒绝响应的 term 属接收方视角，盲目收敛会在假性冲突下
        // 造成 term 无限爬升；任期推进均依赖正式选举/心跳路径）
        // 占位失败（term=0）属"无响应"语义——忽略，靠探测超时兜底进入正式选举
        if (response.getTerm() > ctx.term.getCurrent()) {
            roles.doTransitionTo(NodeState.FOLLOWER);
            ctx.prevoting = false;
        }
    }

    /**
     * 接收方处理预投票探测（raft 单线程内执行）。
     *
     * <p>三条拒绝规则：</p>
     * <ol>
     *   <li>请求探测 term <= 本地当前任期（陈旧探测）；</li>
     *   <li>现任 leader 心跳新鲜（sticky）：健康 leader 存活时不接受任何任期重选；</li>
     *   <li>候选者日志不新于本地（选举安全校验）。</li>
     * </ol>
     */
    PreVoteResponse doHandlePreVoteRequest(PreVoteRequest request) {
        // 跨机房父组：未持有代表席位的节点不代表本机房应答预票。
        // 与 doHandleRequestVote 的席位门控对称——预票虽不改任何持久状态，
        // 但对侧据以判断"本机房是否同意开选"；若机房内三个进程各应一票，
        // 会凭空凑成多数，使低优先级机房的探测恒通过、随后反复进入正式选举抬 term。
        if (!ctx.seatHeld) {
            logger.debug("Node {} holds no representative seat, rejecting pre-vote from {}",
                ctx.nodeId, request.getCandidateId());
            return new PreVoteResponse(request.getRequestId(), ctx.term.getCurrent(), false);
        }

        long localTerm = ctx.term.getCurrent();
        if (request.getTerm() <= localTerm) {
            logger.debug("Node {} rejects pre-vote from {} (stale probe term {} <= {})",
                ctx.nodeId, request.getCandidateId(), request.getTerm(), localTerm);
            return new PreVoteResponse(request.getRequestId(), localTerm, false);
        }

        // sticky：自身已是 Leader（现任即自己），或现任 leader 心跳仍新鲜则拒绝
        // （Leader 的 lastHeartbeatNanos 不由自身心跳刷新，必须显式判定角色）。
        // 权重桥（策略化，三机房 dominant 语义）：候选者机房优先级严格高于现任 Leader
        // 所在机房时 sticky 让位——否则三机房 3/2/2 下初始竞速被低权重机房抢先时，
        // 高权重主机房的夺回通道被永久堵死（term 膨胀防护的前提是"不再选"）。
        // 让位决策全部收敛在 {@code LeadershipPolicy}（peer 缺省恒不让位，零行为变更）；
        // 单向严格（高夺低可、低夺高不可），全网无乒乓收敛到最高权重机房。
        LeadershipPolicy leadership = ctx.leadershipPolicy;
        // 现任 Leader 所在机房：self 即 Leader 时用本机房（peerDatacenters 只登记 peers），
        // 否则按 peer 登记查询（未登记归一空串→ 兜底权重）
        String incumbentLeaderDc;
        if (ctx.nodeId.equals(ctx.leaderId)) {
            incumbentLeaderDc = ctx.datacenter;
        } else if (ctx.leaderId != null) {
            incumbentLeaderDc = quorum.peerDatacenter(ctx.leaderId);
        } else {
            incumbentLeaderDc = null;
        }
        boolean stickyBlocks = ctx.currentState == NodeState.LEADER || isLeaderHeartbeatFresh();
        if (stickyBlocks
            && !leadership.stickyMayYield(request.getDatacenter(), incumbentLeaderDc)) {
            logger.info("Node {} rejects pre-vote from {} (self-leader or live leader {})",
                ctx.nodeId, request.getCandidateId(), ctx.leaderId);
            return new PreVoteResponse(request.getRequestId(), localTerm, false);
        }

        // 日志安全校验：候选者日志必须不旧于本地
        if (request.getLastLogTerm() < logStore.getLastLogTerm()
            || (request.getLastLogTerm() == logStore.getLastLogTerm() && request.getLastLogIndex() < logStore.getLastLogIndex())) {
            logger.info("Node {} rejects pre-vote from {} (log stale: candidate index/term {}<{} vs local {}/{}), ",
                ctx.nodeId, request.getCandidateId(),
                request.getLastLogIndex(), request.getLastLogTerm(),
                logStore.getLastLogIndex(), logStore.getLastLogTerm());
            return new PreVoteResponse(request.getRequestId(), localTerm, false);
        }

        // 机房优先级闸门：与 doHandleRequestVote 同口径，低优先级机房的探测在第一关即被挡下。
        //
        // 预票不修改 term/持久状态，故此处无需像 RequestVote 那样"先对齐 term 再拒绝"——
        // 那条顺序要求是为了让本节点 term 不落后于对侧，而探测根本不推进任期。
        // 挡在预票阶段的收益是：低优先级机房连正式选举都不会进入，term 不再空转攀升。
        //
        // 缺省策略 shouldGrantVote() 恒放行，单机房与既有权重策略行为完全不变。
        if (ctx.voteWeightStrategy != null) {
            String candidateDatacenter = request.getDatacenter() != null
                ? request.getDatacenter()
                : ctx.datacenter;
            VoteContext candidateContext = VoteContext.forDatacenter(
                request.getCandidateId(), candidateDatacenter, probeTermOf(request));
            if (!ctx.voteWeightStrategy.shouldGrantVote(candidateContext)) {
                logger.info(
                    "Node {} (dc={}) refuses pre-vote to {} (dc={}): lower datacenter priority",
                    ctx.nodeId, ctx.datacenter, request.getCandidateId(), candidateDatacenter);
                return new PreVoteResponse(request.getRequestId(), localTerm, false);
            }
        }

        return new PreVoteResponse(request.getRequestId(), localTerm, true);
    }

    /** 预投票上下文任期：优先取探测自带的 probe term，缺失时回退本地当前任期。 */
    private long probeTermOf(PreVoteRequest request) {
        return request.getTerm() > 0 ? request.getTerm() : ctx.term.getCurrent();
    }

    // ==================== 投票受理 ====================

    /**
     * RequestVote 受理（raft 线程内）：
     * <ol>
     *   <li>低 term → 拒绝；</li>
     *   <li>高 term → 任期对齐 + 降级 + 清票；</li>
     *   <li>Phase C stickiness：现任 leader 心跳健康时拒绝同任期投票；</li>
     *   <li>授票 → 持久化（先落盘后可见）+ 重置选举超时。</li>
     * </ol>
     */
    RequestVoteResponse doHandleRequestVote(RequestVoteRequest request) {
        // 阶段五：投票请求携带的优先级快照先行收敛（epoch 更高才采纳，与是否授票无关），
        // 使投票方在分区愈合后能把已提升的优先级表带给对侧。
        adoptPrioritySnapshot(request.getPriorityEpoch(), request.getPriorityWeights());

        // 跨机房父组：未持有代表席位的节点不代表本机房投票。
        // 否则同一机房会出现多份"代表"投票，父组票数被虚增、多数派判定失真。
        if (!ctx.seatHeld) {
            logger.debug("Node {} holds no representative seat, rejecting vote for {}",
                ctx.nodeId, request.getCandidateId());
            return new RequestVoteResponse(ctx.term.getCurrent(), false);
        }

        if (ctx.term.isMonotonicViolation(request.getTerm())) {
            logger.info("Rejecting vote request from {} with lower term {}",
                request.getCandidateId(), request.getTerm());
            return new RequestVoteResponse(ctx.term.getCurrent(), false);
        }

        if (request.getTerm() > ctx.term.getCurrent()) {
            ctx.term.updateIfHigher(request.getTerm());
            if (ctx.currentState != NodeState.FOLLOWER) {
                roles.doTransitionTo(NodeState.FOLLOWER);
            }
            ctx.votedFor = null;
            ctx.votedForTerm = -1;
            ctx.leaderId = null;
        }

        // 机房优先级闸门：只把票投给"优先级不低于本机房"的候选者。
        //
        // 注意本闸门必须放在 term 对齐【之后】：若在此处提前拒绝且不抬 term，
        // 本节点的 term 会长期低于对侧的膨胀 term，待本机房真正发起选举时
        // 反而会被对方以 isMonotonicViolation 拒绝，永远无法按优先级当选。
        //
        // 缺省策略 shouldGrantVote() 恒放行，单机房与既有权重策略行为完全不变。
        if (ctx.voteWeightStrategy != null) {
            String candidateDatacenter = request.getDatacenter() != null
                ? request.getDatacenter()
                : ctx.datacenter;
            VoteContext candidateContext = VoteContext.forDatacenter(
                request.getCandidateId(), candidateDatacenter, request.getTerm());
            if (!ctx.voteWeightStrategy.shouldGrantVote(candidateContext)) {
                logger.info(
                    "Node {} (dc={}) refuses vote to {} (dc={}): lower datacenter priority",
                    ctx.nodeId, ctx.datacenter, request.getCandidateId(), candidateDatacenter);
                return new RequestVoteResponse(ctx.term.getCurrent(), false);
            }
        }

        // Phase C leader stickiness：现任 leader 心跳仍健康时不被同任期请求动摇
        // （更高任期请求在前面已处理为实现 FOLLOWER+term 对齐，正常放行）
        boolean leaderFresh = isLeaderHeartbeatFresh();
        boolean canVote = (ctx.votedFor == null || ctx.votedFor.equals(request.getCandidateId()))
            && ctx.votedForTerm != ctx.term.getCurrent()
            && !(leaderFresh && request.getTerm() < ctx.term.getCurrent());

        if (canVote && request.getTerm() >= ctx.term.getCurrent()) {
            ctx.votedFor = request.getCandidateId();
            ctx.votedForTerm = ctx.term.getCurrent();
            logStore.persistTermState(ctx.term.getCurrent(), ctx.votedFor, ctx.leaderId);
            resetElectionTimeout();
            logger.info("Node {} voted for {} in term {}", ctx.nodeId, request.getCandidateId(), request.getTerm());
            return new RequestVoteResponse(ctx.term.getCurrent(), true);
        }

        logger.info("Node {} rejected vote for {} (votedFor: {}, currentTerm: {})",
            ctx.nodeId, request.getCandidateId(), ctx.votedFor, ctx.term.getCurrent());
        return new RequestVoteResponse(ctx.term.getCurrent(), false);
    }

    // ==================== 选举超时 & 心跳新鲜度 ====================

    /**
     * 选举超时到期后的判定（raft 单线程内执行）：
     * 心跳真超时则发起选举，否则重新武装下一次检测（self-rescheduling）。
     */
    void runElectionTimeout(long timeoutMs) {
        if (ctx.running && ctx.currentState != NodeState.LEADER) {
            // 跨机房父组：未持有代表席位的节点不代表本机房参与选举。
            // 父组成员是各机房的【子组 Leader】，同一机房同时只能有一个代表——
            // 若非代表节点也参与竞选，父组成员数会随子组切换漂移，多数派判定失去意义。
            if (!ctx.seatHeld) {
                resetElectionTimeout();
                return;
            }

            long elapsedNanos = System.nanoTime() - ctx.lastHeartbeatNanos;
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(elapsedNanos);
            // dominant 夺主例外：candidate 权重严格高于现任 Leader 机房时，在位心跳
            // 不再延长本节点的等待（心跳会刷新 lastHeartbeatNanos，elapsed 永远不满、
            // 定时器即便不被 freeze 也进不了 pre-vote）；此时应当机立断发起探测。
            // 判定与 InboundAppendHandler.freezeHeartbeatElectionTimer 同一策略口径。
            boolean takeoverProbe = false;
            if (elapsedMs < timeoutMs && ctx.leaderId != null) {
                LeadershipPolicy policy = ctx.leadershipPolicy;
                String leaderDc = ctx.peerDatacenters.get(ctx.leaderId);
                if (!policy.freezeHeartbeatElectionTimer(leaderDc, ctx.datacenter)) {
                    takeoverProbe = true;
                }
            }
            if (elapsedMs >= timeoutMs || takeoverProbe) {
                // Phase C：先经预投票探测，避免被分区恢复节点以暴涨 term 打奔现行任
                logger.info("Node {} election timeout (elapsed={}ms), starting pre-vote", ctx.nodeId, elapsedMs);
                doStartPreVote();
            } else {
                resetElectionTimeout();
            }
        }
    }

    /** leader 心跳新鲜阈值：取选举超时上限，避免误判抖动 */
    private long heartbeatFreshThresholdMs() {
        return ctx.electionTimeout.getMaxMs();
    }

    private long elapsedLeaderHeartbeatMs() {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - ctx.lastHeartbeatNanos);
    }

    /** 现任 leader 心跳是否新鲜（check-quorum 与 sticky 共用） */
    boolean isLeaderHeartbeatFresh() {
        return ctx.leaderId != null
            && elapsedLeaderHeartbeatMs() < heartbeatFreshThresholdMs();
    }

    /**
     * 重置选举超时（re-arm）：先取消旧 Future，再按随机超时调度
     * {@code ElectionTimeoutTask}。同样语义亦由门面公开（RaftNode.resetElectionTimeout）。
     */
    void resetElectionTimeout() {
        if (ctx.executor == null) {
            return;
        }

        if (ctx.getElectionTimeoutFuture() != null) {
            ctx.getElectionTimeoutFuture().cancel(false);
        }

        ctx.electionTimeout.reset();
        long timeout = ctx.electionTimeout.getNext();

        ctx.setElectionTimeoutFuture(ctx.executor.schedule(
            new ElectionTimeoutTask(() -> runElectionTimeout(timeout)), timeout));
    }

    // ==================== 依赖注入（消环挂点） ====================

    /** apply 推进挂点（启动期由门面注入 ApplyEngine；以对象承载避免间接层散落） */
    interface ApplyBridge {
        void applyCommittedEntries();
    }

    /** 注入方式：字段 + setter（构造循环依赖由门面两段布线：先 election 后 apply） */
    private ApplyBridge applyFlow;

    void bind(ApplyBridge applyFlow) {
        this.applyFlow = applyFlow;
    }
}
