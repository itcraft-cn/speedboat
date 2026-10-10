package cn.itcraft.speedboat.persistence;
import cn.itcraft.speedboat.strategy.voteweight.DatacenterPriorityTable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 父侧机房优先级表独立持久化（阶段五人工升级 API）。
 *
 * <p>把当前生效的优先级快照（{@code epoch} / {@code source} / 机房→权重）写入一个
 * <b>独立小文件</b>（{@code parent-priority.properties}），与 {@code RaftStore}
 * 的 mmap term 档彻底分离——不改动 {@link RaftStore} 接口与其契约测试。父组启动时
 * 若文件存在，则以其为初值覆盖配置派生表，实现"重启不丢人工提升状态"。</p>
 *
 * <h3>线程契约</h3>
 * <p>{@link #save} 仅在父组 raft 线程内被调用（随 PRIORITY_CHANGE apply 或人工提升落地）；
 * {@link #load} 在启动期单线程调用。两者各自持有文件句柄、方法内同步，互不并发。</p>
 *
 * @author speedboat
 * @since 1.2.0
 */
public final class PriorityStore {

    private static final Logger logger = LoggerFactory.getLogger(PriorityStore.class);

    /** 文件名固定：与 storeOwnerId 所在目录组合成完整路径 */
    static final String FILE_NAME = "parent-priority.properties";

    private static final String KEY_EPOCH = "epoch";
    private static final String KEY_SOURCE = "source";
    private static final String KEY_WEIGHT_PREFIX = "weight.";

    private final File file;

    /**
     * @param dir 目录（父组独立目录，通常为 {@code raft.persistence.dir/<父组nodeId>}）
     */
    public PriorityStore(File dir) {
        if (dir == null) {
            throw new IllegalArgumentException("PriorityStore dir cannot be null");
        }
        this.file = new File(dir, FILE_NAME);
    }

    /** 持久化文件绝对路径（诊断用） */
    public File file() {
        return file;
    }

    /**
     * 落盘当前快照（覆盖写；先写临时文件再原子改名，避免半写损坏）。
     *
     * <p>失败仅 WARN 不抛——优先级表本体已在内存生效并随心跳传播，持久化失败
     * 只影响"重启恢复"这一层，不应让 apply 路径中断。</p>
     */
    public synchronized void save(DatacenterPriorityTable.Snapshot snapshot) {
        if (snapshot == null) {
            return;
        }
        Properties props = new Properties();
        props.setProperty(KEY_EPOCH, String.valueOf(snapshot.epoch()));
        props.setProperty(KEY_SOURCE, snapshot.source().name());
        for (Map.Entry<String, Integer> e : snapshot.weights().entrySet()) {
            props.setProperty(KEY_WEIGHT_PREFIX + e.getKey(), String.valueOf(e.getValue()));
        }

        File tmp = new File(file.getParentFile(), FILE_NAME + ".tmp");
        try {
            File parent = file.getParentFile();
            if (parent != null && !parent.exists() && !parent.mkdirs()) {
                logger.warn("PriorityStore cannot create dir {}: file={}", parent, file);
                return;
            }
            try (OutputStream out = new FileOutputStream(tmp)) {
                props.store(out, "speedboat parent datacenter priority (managed by promoteDatacenter; do not edit)");
            }
            if (!tmp.renameTo(file)) {
                // 兜底：部分文件系统 rename 覆盖失败，先删旧文件再改名
                if (file.exists() && !file.delete()) {
                    logger.warn("PriorityStore cannot delete old file {} before rename", file);
                    return;
                }
                if (!tmp.renameTo(file)) {
                    logger.warn("PriorityStore rename failed: tmp={} target={}", tmp, file);
                    return;
                }
            }
            logger.info("PriorityStore saved: file={} epoch={} source={} weights={}",
                file.getAbsolutePath(), snapshot.epoch(), snapshot.source(), snapshot.weights());
        } catch (IOException e) {
            logger.warn("PriorityStore save failed: file={}", file, e);
        } catch (RuntimeException e) {
            // props.store 等也可能抛运行时异常；持久化失败只 WARN 不抛（见方法契约）
            logger.warn("PriorityStore save failed unexpectedly: file={}", file, e);
        } finally {
            // 成功路径 rename 后 tmp 已不存在、失败路径兜底清理——统一在 finally 收口，
            // 避免任何异常路径残留半写临时文件
            if (tmp.exists() && !tmp.delete()) {
                logger.debug("PriorityStore cannot delete tmp file {}", tmp);
            }
        }
    }

    /**
     * 启动期读回快照。
     *
     * @return 恢复出的快照；文件不存在/损坏/为空时返回 null（调用方回退配置派生表）
     */
    public synchronized DatacenterPriorityTable.Snapshot load() {
        if (!file.exists()) {
            logger.info("PriorityStore no file at {}, falling back to config-derived table", file.getAbsolutePath());
            return null;
        }
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(file)) {
            props.load(in);
        } catch (IOException e) {
            logger.warn("PriorityStore load failed: file={}; falling back to config-derived table", file, e);
            return null;
        }

        long epoch;
        try {
            epoch = Long.parseLong(props.getProperty(KEY_EPOCH, "0").trim());
        } catch (NumberFormatException e) {
            logger.warn("PriorityStore bad epoch in {}: {}", file, props.getProperty(KEY_EPOCH));
            return null;
        }
        DatacenterPriorityTable.Source source;
        String sourceName = props.getProperty(KEY_SOURCE, DatacenterPriorityTable.Source.CONFIG.name());
        try {
            source = DatacenterPriorityTable.Source.valueOf(sourceName.trim());
        } catch (IllegalArgumentException e) {
            logger.warn("PriorityStore bad source in {}: {}", file, sourceName);
            source = DatacenterPriorityTable.Source.CONFIG;
        }

        Map<String, Integer> weights = new LinkedHashMap<String, Integer>();
        for (String name : props.stringPropertyNames()) {
            if (!name.startsWith(KEY_WEIGHT_PREFIX)) {
                continue;
            }
            String dc = name.substring(KEY_WEIGHT_PREFIX.length());
            if (dc.isEmpty()) {
                continue;
            }
            try {
                int w = Integer.parseInt(props.getProperty(name).trim());
                if (w > 0) {
                    weights.put(dc, w);
                }
            } catch (NumberFormatException e) {
                logger.warn("PriorityStore bad weight {}={} in {}", name, props.getProperty(name), file);
            }
        }

        logger.info("PriorityStore loaded: file={} epoch={} source={} weights={}",
            file.getAbsolutePath(), epoch, source, weights);
        // 经公共快照工厂产出（不再借临时表 replace 语义搭桥）
        return DatacenterPriorityTable.snapshot(epoch, weights, source);
    }
}
