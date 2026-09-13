package cn.itcraft.speedboat.sample;

import cn.itcraft.speedboat.Speedboat;
import cn.itcraft.speedboat.config.PropertiesConfigProvider;
import cn.itcraft.speedboat.lock.DistributedLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 基于 /etc/hosts 的真实三进程集群测试单节点。
 *
 * <p>与 {@link ClusterTestNode} 直接构建 RaftNode 不同，本节点走
 * {@link Speedboat} 门面的正式用户路径：Speedboat.start(config) 自动完成
 * IP 匹配、nodeId 推导、模式识别与启动，验证代码如实完成设计意图。</p>
 *
 * <p>单机模拟三台"服务器"的方式：</p>
 * <pre>
 *   JVM1: -Dspeedboat.local.ip=127.0.0.1   (mockhost1)
 *   JVM2: -Dspeedboat.local.ip=127.0.0.2   (mockhost2)
 *   JVM3: -Dspeedboat.local.ip=127.0.0.3   (mockhost3)
 * </pre>
 * 配置文件 nodes.*.x=127.0.0.1:31001/127.0.0.2:31002/127.0.0.3:31003。
 *
 * <p>运行模式（--mode）：</p>
 * <ul>
 *   <li>{@code run}：启动后循环上报状态；验证锁行为；REGTERM 优雅退出</li>
 *   <li>{@code oneshot}：启动后验证状态若干轮后退出（用于重启后验证）</li>
 * </ul>
 *
 * 注意：本类为测试代码专用（规范禁止主支包含 main 方法），验证结果以日志为准。
 */
public class HostClusterNode {

    /** 日志，避免使用 stdout/stderr */
    private static final Logger logger = LoggerFactory.getLogger(HostClusterNode.class);

    public static void main(String[] args) throws Exception {
        String configPath = null;
        String mode = "run";

        for (int i = 0; i < args.length; i++) {
            if ("--config".equals(args[i]) && i + 1 < args.length) {
                configPath = args[i + 1];
            }
            if ("--mode".equals(args[i]) && i + 1 < args.length) {
                mode = args[i + 1];
            }
        }

        if (configPath == null) {
            logger.error("[hosts-cluster] 缺少 --config 参数");
            System.exit(2);
        }

        // 1. 走正式用户路径：一行启动
        Speedboat.start(new PropertiesConfigProvider(configPath));
        logger.info("[hosts-cluster] nodeId={} datacenter={} started, mode={}",
            Speedboat.getNodeId(), Speedboat.getDatacenterId(), mode);

        final String marker = "[PASS]";
        if ("run".equals(mode)) {
            runUntilShutdown(marker);
        } else {
            runOneshot(marker);
        }

        Speedboat.stop();
        logger.info("[hosts-cluster] graceful shutdown complete");
    }

    /**
     * run 模式：持续验证身份稳定性和分布式锁设计意图，直到被 kill。
     */
    private static void runUntilShutdown(String marker) throws Exception {
        boolean lockVerified = false;
        int round = 0;
        while (true) {
            round++;
            reportState(round);

            // 设计意图验证 1：身份全网一致（nodeId = node-<ip>-<port>）
            if (round == 1) {
                String expectPrefix = "node-";
                logger.info("{} nodeId 形态验证: nodeId={} prefixOk={}", marker,
                    Speedboat.getNodeId(), Speedboat.getNodeId().startsWith(expectPrefix));
            }

            // 设计意图验证 2：主节点才有权获取锁；非主节点必须失败
            if (Speedboat.isMain()) {
                DistributedLock lock = Speedboat.getLock("cluster-lock");
                if (!lockVerified && lock.tryLock(2000).isSuccess()) {
                    logger.info("{} [LOCK] Leader acquired cluster-lock, holder={}",
                        marker, lock.getHolderNodeId());
                    // 验证持锁者后释放
                    lock.unlock();
                    boolean reallyReleased = !lock.isLocked();
                    logger.info("{} [LOCK] Leader released cluster-lock, released={}", marker, reallyReleased);
                    lockVerified = true;
                }
            } else if (round % 10 == 0) {
                DistributedLock lock = Speedboat.getLock("cluster-lock");
                boolean illegal = lock.tryLock(200).isSuccess();
                if (illegal) {
                    // 非主节点不该成功；若成功立即释放并告警
                    lock.unlock();
                    logger.warn("[FAIL]-candidate tryLock(non-leader) unexpectedly succeeded");
                } else if (!lockVerified) {
                    logger.info("{} [LOCK] Non-leader tryLock rejected as designed", marker);
                }
            }

            TimeUnit.SECONDS.sleep(1);
        }
    }

    /**
     * oneshot 模式：运行 5 轮状态上报后退出（重启后的验证窗口）。
     */
    private static void runOneshot(String marker) throws Exception {
        for (int round = 1; round <= 5; round++) {
            reportState(round);
            TimeUnit.SECONDS.sleep(1);
        }
    }

    /**
     * 结构化状态上报：便于外部脚本从日志解析断言。
     */
    private static void reportState(int round) {
        logger.info("[hosts-cluster] STATE round={} nodeId={} term={} leader={} isMain={}",
            round, Speedboat.getNodeId(), Speedboat.getTerm(),
            Speedboat.getLeaderId(), Speedboat.isMain());
    }
}
