package cn.itcraft.speedboat.sample;

import cn.itcraft.speedboat.Speedboat;
import cn.itcraft.speedboat.config.PropertiesConfigProvider;
import cn.itcraft.speedboat.lock.DistributedLock;
import cn.itcraft.speedboat.lock.LockHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 命名锁真实网络验证节点（2026-09-18 设计 P3 真实网络部分）。
 *
 * <p>走 {@link Speedboat} 正式用户路径启动，验证"非抢占公平竞争"语义下的
 * 多持有者命名锁：任意成员经转发持有任意名目锁、续期、迁移与 epoch fencing。</p>
 *
 * <p><b>运行模式（--mode）：</b></p>
 * <ul>
 *   <li>{@code hold}：--lock NAME，tryLock 成功后长期持有（每秒上报 holder/epoch），
 *       直到被 kill——"持有者失联触发的租约迁移"验证的持有侧</li>
 *   <li>{@code acquire}：--lock NAME，一次性 tryLock(8000)，日志输出 [PASS]/[FAIL]
 *       holder/epoch，随后退出——迁移后的接管者验证</li>
 *   <li>{@code state}（缺省）：等选主并打 3 轮 STATE 后退出——脚本做全网一致性断言</li>
 * </ul>
 *
 * <p>注意：本类为测试代码专用（规范禁止主支包含 main 方法），验证结果以日志为准。</p>
 */
public class NamedLockNode {

    private static final Logger logger = LoggerFactory.getLogger(NamedLockNode.class);

    public static void main(String[] args) throws Exception {
        String configPath = null;
        String mode = "state";
        String lockName = null;

        for (int i = 0; i < args.length; i++) {
            if ("--config".equals(args[i]) && i + 1 < args.length) {
                configPath = args[i + 1];
            }
            if ("--mode".equals(args[i]) && i + 1 < args.length) {
                mode = args[i + 1];
            }
            if ("--lock".equals(args[i]) && i + 1 < args.length) {
                lockName = args[i + 1];
            }
        }

        if (configPath == null) {
            logger.error("[named-lock] 缺少 --config 参数");
            System.exit(2);
        }

        Speedboat.start(new PropertiesConfigProvider(configPath));
        logger.info("[named-lock] nodeId={} started, mode={} lock={}",
            Speedboat.getNodeId(), mode, lockName);

        final String marker = "[PASS]";

        switch (mode) {
            case "hold":
                runHolding(lockName);
                break;
            case "acquire":
                runAcquireOnce(lockName);
                break;
            default:
                runStateRounds();
                break;
        }

        Speedboat.stop();
        logger.info("[named-lock] graceful shutdown complete");
    }

    /**
     * hold 模式：tryLock 成功后仅上报、不释放——非抢占语义下"失联才迁移"，
     * 由外部 kill 驱动迁移；不响应优雅退出（模拟持有者崩溃）。
     */
    private static void runHolding(String lockName) throws Exception {
        DistributedLock lock = Speedboat.getLock(lockName);
        LockHandle handle = lock.tryLock(30000);

        if (!handle.isSuccess()) {
            logger.error("[FAIL]-candidate failed to acquire in hold mode: lock={}, holder={}",
                lockName, Speedboat.getNodeId());
            return;
        }
        logger.info("[PASS]-candidate [LOCK] acquired: lock={}, holder={}, epoch={}",
            lockName, Speedboat.getNodeId(), handle.getEpoch());

        int round = 0;
        while (true) {
            round++;
            logger.info("[hosts-cluster] HOLD round={} lock={} nodeId={} epoch={} holder={} isMain={}",
                round, lockName, Speedboat.getNodeId(), handle.getEpoch(),
                lock.getHolderNodeId(), Speedboat.isMain());
            TimeUnit.SECONDS.sleep(1);
        }
    }

    /**
     * acquire 模式：一次性申请（迁移验证的接管者视角），输出结果后退出。
     */
    private static void runAcquireOnce(String lockName) throws Exception {
        DistributedLock lock = Speedboat.getLock(lockName);
        LockHandle handle = lock.tryLock(30000);

        if (handle.isSuccess()) {
            logger.info("[PASS]-candidate [LOCK] acquired: lock={}, holder={}, epoch={}",
                lockName, Speedboat.getNodeId(), handle.getEpoch());
        } else {
            logger.error("[FAIL]-candidate failed to acquire: lock={}, holder={}",
                lockName, Speedboat.getNodeId());
        }
        handle.close();
    }

    private static void runStateRounds() throws Exception {
        for (int round = 1; round <= 3; round++) {
            logger.info("[hosts-cluster] STATE round={} nodeId={} term={} leader={} isMain={}",
                round, Speedboat.getNodeId(), Speedboat.getTerm(),
                Speedboat.getLeaderId(), Speedboat.isMain());
            TimeUnit.SECONDS.sleep(1);
        }
    }
}
