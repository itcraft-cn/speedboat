package cn.itcraft.speedboat.sample;

import cn.itcraft.speedboat.Speedboat;
import cn.itcraft.speedboat.config.PropertiesConfigProvider;
import cn.itcraft.speedboat.lock.DistributedLock;
import cn.itcraft.speedboat.lock.LockHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 三机虚拟机（vbox）实机验证节点——**官方门面路径**。
 *
 * <p>与 {@link ClusterTestNode} 直接构建 RaftNode 不同，本类走 {@link Speedboat}
 * 正式用户路径：{@code Speedboat.start(PropertiesConfigProvider)} 自动完成本机 IP
 * 匹配、nodeId 推导、单机房/跨机房模式识别与持久化档解析，验证"代码如实完成设计意图"。</p>
 *
 * <p>本机 IP 通过 {@code -Dspeedboat.local.ip=<ip>} 显式钉定（多网卡主机避免枚举顺序漂移）。</p>
 *
 * <p>日志为结构化 key=value，统一前缀 {@code SPEEDBOAT-VM}，供外部编排脚本解析：</p>
 * <ul>
 *   <li>{@code event=START node=<> dc=<> mode=<>} 启动完成</li>
 *   <li>{@code event=STATE node=<> term=<> leader=<> isMain=<>} 状态采样（约 1s 一次）</li>
 *   <li>{@code event=LOCK_ACQUIRE node=<> lock=<> epoch=<>} 成功持锁（含 fencing epoch）</li>
 *   <li>{@code event=LOCK_RELEASE node=<> lock=<> epoch=<>} 主动释放</li>
 *   <li>{@code event=LOCK_REJECT node=<> lock=<>} 本轮未获授权</li>
 *   <li>{@code event=SHUTDOWN/ONESHOT_DONE} 生命周期终止</li>
 * </ul>
 *
 * <p>运行模式（{@code --mode}）：</p>
 * <ul>
 *   <li>{@code run}：常驻。每周期上报状态并竞争命名锁（短持有后释放），直到被 kill</li>
 *   <li>{@code oneshot}：上报 {@code --rounds} 轮状态后优雅退出（用于重启挂回验证）</li>
 * </ul>
 *
 * <p>注意：本类为测试代码专用（遵循规范，禁止主源集包含 main）；验证结论以日志与
 * 编排脚本断言为准，不向 stdout/stderr 输出。</p>
 */
public class VmClusterNode {

    /** 结构化日志前缀，编排脚本据此过滤，避免与框架日志混淆 */
    private static final String MARK = "SPEEDBOAT-VM";

    /** 单次 tryLock 等待窗口：过短会高频 LOCK_REJECT，过长会拉长竞争周期 */
    private static final long LOCK_WAIT_MS = 800L;

    /** 持锁时长：需明显大于锁命令的 Raft 往返，确保其他节点能观察到"被占"并拒绝 */
    private static final long LOCK_HOLD_MS = 1500L;

    /** 每周期额外静默时长，控制竞争烈度与日志量 */
    private static final long CYCLE_GAP_MS = 300L;

    private static final Logger logger = LoggerFactory.getLogger(VmClusterNode.class);

    public static void main(String[] args) throws Exception {
        String configPath = null;
        String mode = "run";
        String lockName = "vm-lock";
        int rounds = 5;

        for (int i = 0; i < args.length; i++) {
            if ("--config".equals(args[i]) && i + 1 < args.length) {
                configPath = args[++i];
            } else if ("--mode".equals(args[i]) && i + 1 < args.length) {
                mode = args[++i];
            } else if ("--lock".equals(args[i]) && i + 1 < args.length) {
                lockName = args[++i];
            } else if ("--rounds".equals(args[i]) && i + 1 < args.length) {
                rounds = Integer.parseInt(args[++i]);
            }
        }

        if (configPath == null) {
            logger.error("{} event=FATAL reason=missing-config (expect --config <path>)", MARK);
            System.exit(2);
        }

        // 优雅停机钩子：SIGTERM 时收尾（mmap 档同步封口、锁与传输层释放）
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("{} event=SHUTDOWN node={}", MARK, safeNodeId());
            try {
                Speedboat.stop();
            } catch (Throwable t) {
                logger.warn("{} event=SHUTDOWN_ERROR reason={}", MARK, t.toString());
            }
        }, "vm-node-shutdown"));

        // 1. 官方门面：一行启动
        Speedboat.start(new PropertiesConfigProvider(configPath));
        logger.info("{} event=START node={} dc={} mode={}", MARK,
            Speedboat.getNodeId(), Speedboat.getDatacenterId(), mode);

        DistributedLock lock = Speedboat.getLock(lockName);

        if ("oneshot".equals(mode)) {
            for (int r = 1; r <= rounds; r++) {
                logState();
                Thread.sleep(1000L);
            }
            logger.info("{} event=ONESHOT_DONE node={}", MARK, Speedboat.getNodeId());
            Speedboat.stop();
            return;
        }

        long cycle = 0;
        while (true) {
            cycle++;
            logState();

            LockHandle handle = null;
            try {
                handle = lock.tryLock(LOCK_WAIT_MS);
            } catch (Throwable t) {
                logger.warn("{} event=LOCK_ERROR node={} cycle={} reason={}", MARK, Speedboat.getNodeId(), cycle, t.toString());
            }

            if (handle != null && handle.isSuccess()) {
                long epoch = handle.getEpoch();
                logger.info("{} event=LOCK_ACQUIRE node={} lock={} epoch={} cycle={}",
                    MARK, Speedboat.getNodeId(), lockName, epoch, cycle);
                try {
                    Thread.sleep(LOCK_HOLD_MS);
                } finally {
                    handle.close();
                    logger.info("{} event=LOCK_RELEASE node={} lock={} epoch={} cycle={}",
                        MARK, Speedboat.getNodeId(), lockName, epoch, cycle);
                }
            } else {
                logger.info("{} event=LOCK_REJECT node={} lock={} cycle={}",
                    MARK, Speedboat.getNodeId(), lockName, cycle);
            }

            Thread.sleep(CYCLE_GAP_MS);
        }
    }

    /** 结构化状态采样：leader 字段为空时输出 none，便于脚本稳定解析。 */
    private static void logState() {
        String leader = Speedboat.getLeaderId();
        logger.info("{} event=STATE node={} term={} leader={} isMain={}",
            MARK, Speedboat.getNodeId(), Speedboat.getTerm(),
            leader == null ? "none" : leader, Speedboat.isMain());
    }

    private static String safeNodeId() {
        try {
            return Speedboat.getNodeId();
        } catch (Throwable t) {
            return "unknown";
        }
    }
}
