package cn.itcraft.speedboat.raft;
import cn.itcraft.speedboat.config.PropertiesConfigProvider;
import cn.itcraft.speedboat.config.SpeedboatConfigProvider;
import cn.itcraft.speedboat.strategy.consistency.ApConsistencyPolicy;
import cn.itcraft.speedboat.strategy.consistency.ConsistencyPolicy;
import cn.itcraft.speedboat.strategy.consistency.CpConsistencyPolicy;
import cn.itcraft.speedboat.strategy.voteweight.DatacenterPriorityVoteWeightStrategy;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
/**
 * 一致性策略（CP/AP）与 AP 降级分母收缩测试。
 *
 * <p><b>被守护的不变式：</b></p>
 * <ol>
 *   <li><b>CP 路径逐字节等价</b>——CP 策略的 {@code excludedDatacenters()} 恒为空集，
 *       分母聚合逻辑读到空集即跳过剔除分支，{@code total/required} 与引入策略前完全一致。
 *       这是"新增策略零回归"的核心保证，既有单机房与六机实机回归必须原样通过。</li>
 *   <li><b>AP 降级收缩分母</b>——对侧机房失联达阈值后，{@code excludedDatacenters} 含对侧机房，
 *       分母收缩为本机房权重，使备机房 {@code self(1) >= required(1)} 可单方成主。</li>
 *   <li><b>AP 退出恢复全量分母</b>——对侧恢复应答后退出降级，分母回到全量，标准 Raft 收敛。</li>
 *   <li><b>降级前置条件</b>——无对侧机房（单机房集群）或无代表席位时，AP 永不降级。</li>
 * </ol>
 *
 * <p>全部走纯函数路径，不启动节点、不依赖真实时序（时间戳由测试显式喂入），结果确定可复现。</p>
 *
 * @author speedboat
 * @since 1.2.0
 */
class ConsistencyPolicyTest {

    private static final String MAIN_DC = "dc-0";
    private static final String BACKUP_DC = "dc-1";

    /** 设计给定机房权重表：W_main=2 / W_backup=1 */
    private static Map<String, Integer> weightsByDatacenter() {
        Map<String, Integer> weights = new LinkedHashMap<String, Integer>();
        weights.put(MAIN_DC, 2);
        weights.put(BACKUP_DC, 1);
        return weights;
    }

    /**
     * 父组装配：上下文 + 计算器 + peer 清单，可注入一致性策略。
     */
    private static final class ParentGroup {

        final NodeContext ctx;
        final QuorumCalculator quorum;
        final List<String> peerIds;

        ParentGroup(String localDatacenter, String peerDatacenter, int peerCount,
                    ConsistencyPolicy policy, boolean seatHeld) {
            this.peerIds = new ArrayList<String>();
            for (int i = 0; i < peerCount; i++) {
                peerIds.add("node-192.168.193.5" + i + "-22001");
            }

            RaftNode.Builder builder = new RaftNode.Builder()
                .nodeId("node-192.168.193.174-22001")
                .peerIds(peerIds)
                .electionTimeout(new ElectionTimeout(3000, 5000))
                .datacenter(localDatacenter)
                .voteWeightStrategy(new DatacenterPriorityVoteWeightStrategy(
                    weightsByDatacenter(), localDatacenter, 1))
                .consistencyPolicy(policy);

            this.ctx = new NodeContext(builder);
            this.ctx.seatHeld = seatHeld;
            this.quorum = new QuorumCalculator(ctx);
            for (String peerId : peerIds) {
                quorum.setPeerDatacenter(peerId, peerDatacenter);
            }
        }

        /** 缺省：CP 策略、有席位 */
        static ParentGroup cp(String localDc, String peerDc, int peerCount) {
            return new ParentGroup(localDc, peerDc, peerCount,
                CpConsistencyPolicy.getInstance(), true);
        }

        /** AP 策略、有席位 */
        static ParentGroup ap(String localDc, String peerDc, int peerCount, long timeoutMs) {
            return new ParentGroup(localDc, peerDc, peerCount,
                new ApConsistencyPolicy(timeoutMs), true);
        }
    }

    // ==================== CP 不变式：逐字节等价 ====================

    @Test
    @DisplayName("CP - excludedDatacenters 恒为空集，永不降级")
    void cpNeverExcludesNorDegrades() {
        ConsistencyPolicy cp = CpConsistencyPolicy.getInstance();

        assertEquals(ConsistencyPolicy.MODE_CP, cp.mode());
        assertFalse(cp.allowDegradedTakeover(), "CP 不允许降级接管");
        assertFalse(cp.isDegraded(), "CP 永不进入降级态");
        assertTrue(cp.excludedDatacenters().isEmpty(), "CP 恒不剔除任何机房");
        // 即便调用方喂入"对侧静默极久"，CP 也恒 no-op 且返回 false
        assertFalse(cp.evaluateDegraded(MAIN_DC, BACKUP_DC, true,
            0L, TimeUnit.SECONDS.toNanos(100)));
        assertTrue(cp.excludedDatacenters().isEmpty(), "CP 评估后仍恒为空集");
    }

    @Test
    @DisplayName("CP - 分母不因策略调用而改变（total=3 / required=2）")
    void cpDenominatorUnchangedAfterEvaluate() {
        ParentGroup main = ParentGroup.cp(MAIN_DC, BACKUP_DC, 3);

        // 调用前
        assertEquals(3, main.quorum.totalWeight());
        assertEquals(2, main.quorum.requiredWeight());

        // 模拟 leader 侧降级点调用（CP 恒 no-op）
        main.ctx.consistencyPolicy.evaluateDegraded(MAIN_DC, BACKUP_DC, true,
            0L, TimeUnit.SECONDS.toNanos(100));

        assertEquals(3, main.quorum.totalWeight(), "CP 分母不得因策略调用而收缩");
        assertEquals(2, main.quorum.requiredWeight(), "CP required 不得因策略调用而下降");
        assertTrue(main.quorum.freshWeight(new HashSet<String>(), main.ctx.term.getCurrent()) >= 1,
            "CP 空新鲜集合仍应计自身权重");
    }

    // ==================== AP 降级：进入 / 收缩分母 ====================

    @Test
    @DisplayName("AP - 对侧静默达阈值后进入降级态，剔除对侧机房")
    void apEntersDegradedWhenOppositeSilent() {
        ApConsistencyPolicy ap = new ApConsistencyPolicy(10_000L);
        long now = TimeUnit.SECONDS.toNanos(1000);
        long silentSince = now - TimeUnit.MILLISECONDS.toNanos(10_500);

        // 未达阈值：不降级
        assertFalse(ap.evaluateDegraded(BACKUP_DC, MAIN_DC, true,
            now - TimeUnit.MILLISECONDS.toNanos(9_000), now), "未达阈值不应降级");
        assertFalse(ap.isDegraded());
        assertTrue(ap.excludedDatacenters().isEmpty());

        // 达阈值：进入降级，剔除对侧（主机房）
        assertTrue(ap.evaluateDegraded(BACKUP_DC, MAIN_DC, true, silentSince, now),
            "达阈值应进入降级态");
        assertTrue(ap.isDegraded());
        assertEquals(new HashSet<String>(Arrays.asList(MAIN_DC)), ap.excludedDatacenters(),
            "降级态应剔除对侧（主机房）");
    }

    @Test
    @DisplayName("AP - 降级后备机房分母收缩，self(1) >= required(1) 可单方成主")
    void apDegradedShrinksDenominatorSoBackupCanSelfElect() {
        ParentGroup backup = ParentGroup.ap(BACKUP_DC, MAIN_DC, 3, 10_000L);

        // 降级前：total=3 / required=2，备机房 self=1 < 2 无法成主
        assertEquals(3, backup.quorum.totalWeight(), "降级前分母=全量机房权重");
        assertEquals(2, backup.quorum.requiredWeight());
        assertTrue(backup.quorum.selfWeight(backup.ctx.term.getCurrent()) < backup.quorum.requiredWeight(),
            "降级前备机房 self=1 < required=2 不可成主");

        // 触发降级（对侧主机房静默 10.5s > 阈值 10s）
        long now = TimeUnit.SECONDS.toNanos(1000);
        long silentSince = now - TimeUnit.MILLISECONDS.toNanos(10_500);
        assertTrue(backup.ctx.consistencyPolicy.evaluateDegraded(BACKUP_DC, MAIN_DC, true, silentSince, now));

        // 降级后：分母收缩为备机房权重 1，required=1/2+1=1，self=1 >= 1 可成主
        assertEquals(1, backup.quorum.totalWeight(), "降级后分母收缩为本机房权重");
        assertEquals(1, backup.quorum.requiredWeight(), "降级后 required=1/2+1=1");
        assertTrue(backup.quorum.selfWeight(backup.ctx.term.getCurrent()) >= backup.quorum.requiredWeight(),
            "降级后备机房 self=1 >= required=1 可单方成主");
    }

    @Test
    @DisplayName("AP - 分子分母同口径剔除，对侧部分新鲜时不虚增分子")
    void apFreshWeightExcludesOppositeConsistently() {
        ParentGroup backup = ParentGroup.ap(BACKUP_DC, MAIN_DC, 3, 10_000L);
        long now = TimeUnit.SECONDS.toNanos(1000);
        long silentSince = now - TimeUnit.MILLISECONDS.toNanos(10_500);
        assertTrue(backup.ctx.consistencyPolicy.evaluateDegraded(BACKUP_DC, MAIN_DC, true, silentSince, now));

        // 构造"对侧某个 peer 标记为新鲜"的集合——AP 降级态下分子须同样剔除对侧
        Set<String> freshPeers = new HashSet<String>(backup.peerIds);
        long freshWeight = backup.quorum.freshWeight(freshPeers, backup.ctx.term.getCurrent());
        assertEquals(1, freshWeight, "降级后分子只计本机房自身权重，对侧即使新鲜也不计入");
        assertTrue(freshWeight >= backup.quorum.requiredWeight(), "降级后 freshWeight >= required 不误降级");
    }

    // ==================== AP 退出 / 前置条件 ====================

    @Test
    @DisplayName("AP - 对侧恢复应答后退出降级，分母恢复全量")
    void apExitsDegradedWhenOppositeRecovers() {
        ApConsistencyPolicy ap = new ApConsistencyPolicy(10_000L);
        long now = TimeUnit.SECONDS.toNanos(1000);

        assertTrue(ap.evaluateDegraded(BACKUP_DC, MAIN_DC, true,
            now - TimeUnit.MILLISECONDS.toNanos(10_500), now));
        assertTrue(ap.isDegraded());

        // 对侧恢复（静默时长回落到阈值以下）
        long recovered = now - TimeUnit.MILLISECONDS.toNanos(1_000);
        assertFalse(ap.evaluateDegraded(BACKUP_DC, MAIN_DC, true, recovered, now + TimeUnit.SECONDS.toNanos(1)),
            "对侧恢复后应退出降级");
        assertFalse(ap.isDegraded());
        assertTrue(ap.excludedDatacenters().isEmpty(), "退出后分母恢复全量");
    }

    @Test
    @DisplayName("AP - 单机房集群（无对侧）永不降级")
    void apNeverDegradesWithoutOppositeDatacenter() {
        ApConsistencyPolicy ap = new ApConsistencyPolicy(10_000L);
        long now = TimeUnit.SECONDS.toNanos(1000);

        // opposite=null：同机房集群，无对侧可剔除
        assertFalse(ap.evaluateDegraded(MAIN_DC, null, true, 0L, now), "无对侧机房不应降级");
        // opposite==self：同样无对侧
        assertFalse(ap.evaluateDegraded(MAIN_DC, MAIN_DC, true, 0L, now), "对侧==本机房不应降级");
        assertFalse(ap.isDegraded());
        assertTrue(ap.excludedDatacenters().isEmpty());
    }

    @Test
    @DisplayName("AP - 无代表席位时不降级（不能代表本机房成主）")
    void apNeverDegradesWithoutSeat() {
        ApConsistencyPolicy ap = new ApConsistencyPolicy(10_000L);
        long now = TimeUnit.SECONDS.toNanos(1000);

        assertFalse(ap.evaluateDegraded(BACKUP_DC, MAIN_DC, false, 0L, now),
            "无席位（learner）不应降级接管");
        assertFalse(ap.isDegraded());
        assertTrue(ap.excludedDatacenters().isEmpty());
    }

    @Test
    @DisplayName("AP - reset 退出降级并复位")
    void apResetClearsDegradedState() {
        ApConsistencyPolicy ap = new ApConsistencyPolicy(10_000L);
        long now = TimeUnit.SECONDS.toNanos(1000);

        assertTrue(ap.evaluateDegraded(BACKUP_DC, MAIN_DC, true,
            now - TimeUnit.MILLISECONDS.toNanos(10_500), now));
        assertTrue(ap.isDegraded());

        ap.reset();
        assertFalse(ap.isDegraded(), "reset 后应退出降级");
        assertTrue(ap.excludedDatacenters().isEmpty(), "reset 后剔除集合应清空");
    }

    // ==================== NodeContext 缺省 ====================

    @Test
    @DisplayName("NodeContext 未注入策略时缺省 CP（单机房/既有路径零回归）")
    void nodeContextDefaultsToCpWhenPolicyAbsent() {
        RaftNode.Builder builder = new RaftNode.Builder()
            .nodeId("node-127.0.0.1-21001")
            .peerIds(new ArrayList<String>())
            .electionTimeout(new ElectionTimeout(1000, 2000));
        NodeContext ctx = new NodeContext(builder);

        assertNotNull(ctx.consistencyPolicy, "缺省必须兜底为 CP 策略");
        assertEquals(ConsistencyPolicy.MODE_CP, ctx.consistencyPolicy.mode());
        assertTrue(ctx.consistencyPolicy.excludedDatacenters().isEmpty());
        assertFalse(ctx.consistencyPolicy.isDegraded());
    }

    // ==================== 配置解析 ====================

    /**
     * 以配置片段构造 {@link PropertiesConfigProvider}。
     *
     * <p>该类只接受路径或输入流，测试用内存输入流加载键值片段，避免落临时文件。</p>
     *
     * @param content properties 键值片段（每行 {@code key=value}）
     * @return 配置提供者
     */
    private static SpeedboatConfigProvider providerOf(String content) {
        InputStream in = new ByteArrayInputStream(
            content.getBytes(StandardCharsets.ISO_8859_1));
        return new PropertiesConfigProvider(in);
    }

    @Test
    @DisplayName("配置 - 未配置 consistency.policy 时默认 CP")
    void configDefaultsToCp() {
        SpeedboatConfigProvider provider = providerOf("");

        ConsistencyPolicy policy = provider.getConsistencyPolicy();
        assertNotNull(policy);
        assertEquals(ConsistencyPolicy.MODE_CP, policy.mode());
        assertFalse(policy.allowDegradedTakeover());
    }

    @Test
    @DisplayName("配置 - consistency.policy=ap 时构造 AP 且读取降级阈值")
    void configParsesApWithTimeout() {
        SpeedboatConfigProvider provider = providerOf(
            "consistency.policy=ap\nconsistency.degraded.timeout.ms=15000\n");

        ConsistencyPolicy policy = provider.getConsistencyPolicy();
        assertEquals(ConsistencyPolicy.MODE_AP, policy.mode());
        assertTrue(policy.allowDegradedTakeover());
        assertEquals(15_000L, policy.degradedTimeoutMs(), "应读取配置的降级阈值");
    }

    @Test
    @DisplayName("配置 - AP 未设阈值时用缺省 10s；非法值回退缺省")
    void configApFallbackTimeout() {
        ConsistencyPolicy apNoTimeout = providerOf("consistency.policy=ap\n").getConsistencyPolicy();
        assertEquals(ApConsistencyPolicy.DEFAULT_DEGRADED_TIMEOUT_MS, apNoTimeout.degradedTimeoutMs(),
            "未配置阈值时应取缺省 10s");

        ConsistencyPolicy apInvalid = providerOf(
            "consistency.policy=ap\nconsistency.degraded.timeout.ms=not-a-number\n")
            .getConsistencyPolicy();
        assertEquals(ApConsistencyPolicy.DEFAULT_DEGRADED_TIMEOUT_MS, apInvalid.degradedTimeoutMs(),
            "非法阈值应回退缺省 10s");
    }

    @Test
    @DisplayName("配置 - 未知策略值回退 CP（不误启用 AP）")
    void configUnknownFallsBackToCp() {
        ConsistencyPolicy policy = providerOf("consistency.policy=unknown-mode\n")
            .getConsistencyPolicy();

        assertEquals(ConsistencyPolicy.MODE_CP, policy.mode(), "未知值必须回退 CP，不得误启用 AP");
        assertFalse(policy.allowDegradedTakeover());
    }
}
