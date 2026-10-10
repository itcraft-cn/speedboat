package cn.itcraft.speedboat.strategy.voteweight;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 机房优先级表（可运行时替换的 volatile 不可变快照）。
 *
 * <p>引入动机：人工升级 API（阶段五）需要在<b>运行时</b>改写父组机房权重。既有
 * {@link DatacenterPriorityVoteWeightStrategy} 在构造期持有不可变 Map，无法变更。
 * 本表把"当前生效的权重"收敛为一个可原子替换的快照，策略每次投票/定主时读取
 * 最新快照，从而在不加锁的前提下支持热更新。</p>
 *
 * <h3>并发契约</h3>
 * <p>写（换快照）仅发生在父组 raft 线程内（Actor 纪律）；读可发生在任意线程。
 * 依靠 {@code volatile} 保证可见性——读方要么看到换表前的完整旧快照、要么看到
 * 换表后的完整新快照，绝不会看到半新半旧的中间态（快照本身构造后不可变）。</p>
 *
 * <h3>epoch 单调</h3>
 * <p>每次换表携带一个单调递增的 {@code epoch}。心跳/投票传播中，接收方仅当
 * {@code 收到的 epoch > 本表 epoch} 才采纳，构成"只升不降"的收敛，避免旧快照
 * 回灌把已提升的机房优先级打回。</p>
 *
 * @author speedboat
 * @since 1.2.0
 */
public final class DatacenterPriorityTable {

    /** 优先级来源：配置派生 / 人工提升 */
    public enum Source {
        /** 配置文件派生（默认优先级） */
        CONFIG,
        /** 人工提升（promoteDatacenter） */
        PROMOTED
    }

    /** 不可变快照：epoch + 权重表 + 来源。构造后不可变，读方拿到的是完整一致视图。 */
    public static final class Snapshot {
        private final long epoch;
        private final Map<String, Integer> weights;
        private final Source source;

        Snapshot(long epoch, Map<String, Integer> weights, Source source) {
            this.epoch = epoch;
            this.weights = weights;
            this.source = source;
        }

        /** 单调递增的版本号 */
        public long epoch() {
            return epoch;
        }

        /** 机房 → 权重（不可变视图） */
        public Map<String, Integer> weights() {
            return weights;
        }

        /** 来源（配置 / 人工） */
        public Source source() {
            return source;
        }

        @Override
        public String toString() {
            return "epoch=" + epoch + " source=" + source + " weights=" + weights;
        }
    }

    /**
     * 直接构造不可变快照（持久层读回场景用：WAL 镜像、独立优先级文件）。
     *
     * <p>独立于 {@link #replace}——持久层不应为读回一次快照而实例化并篡改一个临时表，
     * 本工厂把该"跨包搭桥"收为己有职责。</p>
     *
     * @param epoch   版本号
     * @param weights 权重表（内部复制为不可变）
     * @param source  来源
     * @return 不可变快照
     */
    public static Snapshot snapshot(long epoch, Map<String, Integer> weights, Source source) {
        return new Snapshot(epoch, immutableCopy(weights), source);
    }

    /** 当前生效快照（volatile 保证换表可见性；快照本身不可变故读无需加锁） */
    private volatile Snapshot current;
    /** 配置派生的初始权重表（不可变；restoreDefaultPriorities 回退目标） */
    private final Map<String, Integer> configWeights;

    /**
     * @param initialWeights 初始权重表（配置派生）；null 视为空表
     * @param initialSource  初始来源（通常为 {@link Source#CONFIG}）
     */
    public DatacenterPriorityTable(Map<String, Integer> initialWeights, Source initialSource) {
        Map<String, Integer> cfg = immutableCopy(initialWeights);
        this.configWeights = cfg;
        this.current = new Snapshot(0L, cfg, initialSource);
    }

    /** 配置派生的初始权重表（不可变视图；回退目标） */
    public Map<String, Integer> configWeights() {
        return configWeights;
    }

    /** 当前快照（任意线程可读） */
    public Snapshot current() {
        return current;
    }

    /**
     * 原子换表（仅父组 raft 线程调用）。
     *
     * @param newEpoch  新 epoch（调用方负责单调递增；本方法不强制校验大小）
     * @param newWeights 新权重表；null 视为空表
     * @param source    新来源
     * @return 采纳后的快照
     */
    public Snapshot replace(long newEpoch, Map<String, Integer> newWeights, Source source) {
        Snapshot next = new Snapshot(newEpoch, immutableCopy(newWeights), source);
        this.current = next;
        return next;
    }

    /**
     * 依传播快照收敛（心跳/投票携带）：仅当 {@code remoteEpoch > 本表 epoch} 才采纳。
     *
     * <p>构成"只升不降"的单调收敛：旧快照（含已被提升前的低优先级）永远无法回灌。</p>
     *
     * @param remoteEpoch  远端携带的 epoch
     * @param remoteWeights 远端携带的权重表
     * @param remoteSource 远端携带的来源
     * @return true 表示采纳了新快照；false 表示 epoch 不更高、忽略
     */
    public boolean convergeIfNewer(long remoteEpoch, Map<String, Integer> remoteWeights,
                                   Source remoteSource) {
        Snapshot local = this.current;
        if (remoteEpoch <= local.epoch()) {
            return false;
        }
        this.current = new Snapshot(remoteEpoch, immutableCopy(remoteWeights), remoteSource);
        return true;
    }

    private static Map<String, Integer> immutableCopy(Map<String, Integer> src) {
        if (src == null || src.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<String, Integer>(src));
    }
}
