package cn.itcraft.speedboat.config;
import cn.itcraft.speedboat.config.SpeedboatConsts;
import cn.itcraft.speedboat.strategy.consistency.ApConsistencyPolicy;
import cn.itcraft.speedboat.strategy.consistency.ConsistencyPolicy;
import cn.itcraft.speedboat.strategy.consistency.CpConsistencyPolicy;
import cn.itcraft.speedboat.strategy.leadership.LeadershipPolicy;
import cn.itcraft.speedboat.strategy.voteweight.EvenNodeVoteWeightStrategy;
import cn.itcraft.speedboat.strategy.voteweight.PreferNodeVoteWeightStrategy;
import cn.itcraft.speedboat.strategy.voteweight.VoteWeightStrategy;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Properties 配置提供者（默认实现，零依赖）
 * 
 * <p>使用示例：</p>
 * <pre>{@code
 * // config.properties 内容：
 * // datacenter=hangzhou001
 * // nodes.0.0=192.168.10.1:3000
 * // nodes.0.1=192.168.10.2:3000
 * // nodes.0.2=192.168.10.3:3000
 * 
 * SpeedboatConfigProvider config = new PropertiesConfigProvider("config.properties");
 * Speedboat.start(config);
 * }</pre>
 *
 * <p>Properties 文件格式：</p>
 * <pre>
 * # 机房ID（可选）
 * datacenter=hangzhou001
 * 
 * # 集群节点列表（必需）
 * # 格式：nodes.{机房索引}.{节点索引}=ip:port
 * nodes.0.0=192.168.10.1:3000
 * nodes.0.1=192.168.10.2:3000
 * nodes.0.2=192.168.10.3:3000
 * nodes.1.0=10.3.1.1:3000
 * nodes.1.1=10.3.1.2:3000
 * nodes.1.2=10.3.1.3:3000
 * 
 * # 可选：机房内选举超时
 * election.intra.timeout.min=1000
 * election.intra.timeout.max=2000
 * 
 * # 可选：机房间选举超时
 * election.cross.timeout.min=3000
 * election.cross.timeout.max=5000
 * </pre>
 *
 * @author speedboat
 * @since 1.0.0
 */
public class PropertiesConfigProvider implements SpeedboatConfigProvider {
    
    private static final Logger logger = LoggerFactory.getLogger(PropertiesConfigProvider.class);
    
    private final Properties properties;
    
    public PropertiesConfigProvider(String configFile) {
        this.properties = new Properties();
        loadConfig(configFile);
    }
    
    public PropertiesConfigProvider(InputStream inputStream) {
        this.properties = new Properties();
        loadConfig(inputStream);
    }

    /**
     * 内存日志上限（raft.max.log.size；defect-20260918-01：重启副本唯一重建依据不可截断过深）。
     */
    @Override
    public int getMaxLogSize() {
        String v = properties.getProperty("raft.max.log.size");
        if (v == null || v.isEmpty()) {
            return SpeedboatConsts.DEFAULT_MAX_LOG_SIZE;
        }
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            logger.warn("Invalid raft.max.log.size='{}', fallback to default {}", v, SpeedboatConsts.DEFAULT_MAX_LOG_SIZE);
            return SpeedboatConsts.DEFAULT_MAX_LOG_SIZE;
        }
    }
    
    private void loadConfig(String configFile) {
        try (FileInputStream fis = new FileInputStream(configFile)) {
            loadConfig(fis);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to load config file: " + configFile, e);
        }
    }
    
    private void loadConfig(InputStream inputStream) {
        try {
            properties.load(inputStream);
            logger.info("Loaded configuration from properties file");
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to load config from input stream", e);
        }
    }
    
    @Override
    public String getDatacenter() {
        return properties.getProperty("datacenter");
    }
    
    @Override
    public List<List<String>> getNodes() {
        List<List<String>> nodes = new ArrayList<>();
        
        int datacenterIndex = 0;
        while (true) {
            List<String> datacenterNodes = parseDatacenterNodes(datacenterIndex);
            if (datacenterNodes.isEmpty()) {
                break;
            }
            nodes.add(datacenterNodes);
            datacenterIndex++;
        }
        
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("No nodes configured in properties file");
        }
        
        return nodes;
    }
    
    private List<String> parseDatacenterNodes(int datacenterIndex) {
        List<String> nodes = new ArrayList<>();
        
        int nodeIndex = 0;
        while (true) {
            String key = String.format("nodes.%d.%d", datacenterIndex, nodeIndex);
            String value = properties.getProperty(key);
            if (value == null) {
                break;
            }
            nodes.add(value);
            nodeIndex++;
        }
        
        return nodes;
    }
    
    @Override
    public VoteWeightStrategy getVoteWeightStrategy() {
        String mode = properties.getProperty("vote.weight.strategy");
        if (mode == null) {
            return null;
        }
        String modeVal = mode.trim().toLowerCase();
        if ("none".equals(modeVal)) {
            return null;
        }
        if ("prefer".equals(modeVal)) {
            String prefer = properties.getProperty("vote.weight.prefer");
            if (prefer == null || prefer.trim().isEmpty()) {
                throw new IllegalArgumentException("vote.weight.strategy=prefer requires vote.weight.prefer=<nodeId>");
            }
            String weight = properties.getProperty("vote.weight.prefer.weight", "3");
            int extra = Integer.parseInt(weight.trim());
            return new PreferNodeVoteWeightStrategy(prefer.trim(), extra);
        }
        if ("even".equals(modeVal)) {
            return new EvenNodeVoteWeightStrategy();
        }
        throw new IllegalArgumentException("Unknown vote.weight.strategy: " + modeVal);
    }

    @Override
    public MembershipConfig getMembershipConfig() {
        String autoRemoval = properties.getProperty("membership.auto.removal");
        if (autoRemoval == null) {
            return new MembershipConfig();
        }
        boolean enable = Boolean.parseBoolean(autoRemoval.trim());
        MembershipConfig def = new MembershipConfig();
        return new MembershipConfig(
            def.getHealthCheckIntervalMs(), def.getFailureThreshold(), def.getConfirmationPeriods(),
            def.getRegistryCheckIntervalMs(), def.getRegistryImpl(), enable);
    }

    @Override
    public ConsistencyPolicy getConsistencyPolicy() {
        String policy = properties.getProperty("consistency.policy");
        if (policy == null || policy.trim().isEmpty()) {
            return CpConsistencyPolicy.getInstance();
        }

        String normalized = policy.trim().toLowerCase();
        if (ConsistencyPolicy.MODE_AP.equals(normalized)) {
            // 仅 AP 需读取降级阈值；CP 恒不参与降级判定，不读该键
            long timeoutMs = 0L;
            String timeoutValue = properties.getProperty("consistency.degraded.timeout.ms");
            if (timeoutValue != null) {
                try {
                    timeoutMs = Long.parseLong(timeoutValue.trim());
                } catch (NumberFormatException e) {
                    logger.warn("Invalid consistency.degraded.timeout.ms value: {}, using default {}",
                        timeoutValue, ApConsistencyPolicy.DEFAULT_DEGRADED_TIMEOUT_MS);
                }
            }
            return new ApConsistencyPolicy(timeoutMs);
        }

        if (ConsistencyPolicy.MODE_CP.equals(normalized)) {
            return CpConsistencyPolicy.getInstance();
        }

        logger.warn("Unknown consistency.policy value: {}, falling back to CP", policy);
        return CpConsistencyPolicy.getInstance();
    }

    /**
     * 领袖优先级模式（三机房拓扑语义），对应配置键 {@code crossdc.leadership}。
     *
     * @return 模式标识 peer|dominant；未配置/非法时回退 peer（零行为变更）
     */
    @Override
    public String getLeadershipMode() {
        String value = properties.getProperty("crossdc.leadership");
        if (value == null || value.trim().isEmpty()) {
            return LeadershipPolicy.MODE_PEER;
        }
        String normalized = value.trim().toLowerCase();
        if (LeadershipPolicy.MODE_DOMINANT.equals(normalized)
            || LeadershipPolicy.MODE_PEER.equals(normalized)) {
            return normalized;
        }
        logger.warn("Unknown crossdc.leadership value: {}, falling back to peer", value);
        return LeadershipPolicy.MODE_PEER;
    }


    @Override
    public int getIntraDatacenterElectionTimeoutMin() {
        String value = properties.getProperty("election.intra.timeout.min");
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                logger.warn("Invalid election.intra.timeout.min value: {}, using default 1000", value);
            }
        }
        return SpeedboatConfigProvider.super.getIntraDatacenterElectionTimeoutMin();
    }
    
    @Override
    public int getIntraDatacenterElectionTimeoutMax() {
        String value = properties.getProperty("election.intra.timeout.max");
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                logger.warn("Invalid election.intra.timeout.max value: {}, using default 2000", value);
            }
        }
        return SpeedboatConfigProvider.super.getIntraDatacenterElectionTimeoutMax();
    }
    
    @Override
    public int getCrossDatacenterElectionTimeoutMin() {
        String value = properties.getProperty("election.cross.timeout.min");
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                logger.warn("Invalid election.cross.timeout.min value: {}, using default 3000", value);
            }
        }
        return SpeedboatConfigProvider.super.getCrossDatacenterElectionTimeoutMin();
    }
    
    @Override
    public int getCrossDatacenterElectionTimeoutMax() {
        String value = properties.getProperty("election.cross.timeout.max");
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                logger.warn("Invalid election.cross.timeout.max value: {}, using default 5000", value);
            }
        }
        return SpeedboatConfigProvider.super.getCrossDatacenterElectionTimeoutMax();
    }

    /**
     * 父组端口偏移（跨机房级联用），对应配置键 {@code cross.port.offset}。
     *
     * @return 端口偏移量；未配置或非法时取接口缺省 1000
     */
    @Override
    public int getCrossPortOffset() {
        String value = properties.getProperty("cross.port.offset");
        if (value != null) {
            try {
                int offset = Integer.parseInt(value.trim());
                if (offset >= 0) {
                    return offset;
                }
                logger.warn("Invalid cross.port.offset value: {} (must be >= 0), using default 1000", value);
            } catch (NumberFormatException e) {
                logger.warn("Invalid cross.port.offset value: {}, using default 1000", value);
            }
        }
        return SpeedboatConfigProvider.super.getCrossPortOffset();
    }

    /**
     * 机房权重显式配置，对应配置键前缀 {@code datacenter.weight.<机房标识>}。
     *
     * <p>示例：{@code datacenter.weight.dc-0=3}、{@code datacenter.weight.dc-1=1}。
     * 返回空 Map 表示未显式配置，由门面按机房索引派生。</p>
     *
     * @return 机房标识 → 权重；无合法配置时为空 Map
     */
    @Override
    public Map<String, Integer> getDatacenterWeights() {
        final String prefix = "datacenter.weight.";
        Map<String, Integer> weights = new HashMap<>();

        for (String name : properties.stringPropertyNames()) {
            if (!name.startsWith(prefix)) {
                continue;
            }
            String datacenterId = name.substring(prefix.length());
            if (datacenterId.isEmpty()) {
                continue;
            }
            String value = properties.getProperty(name);
            try {
                int weight = Integer.parseInt(value.trim());
                if (weight > 0) {
                    weights.put(datacenterId, weight);
                } else {
                    logger.warn("Invalid {} value: {} (must be > 0), ignoring", name, value);
                }
            } catch (NumberFormatException e) {
                logger.warn("Invalid {} value: {}, ignoring", name, value);
            }
        }
        return weights;
    }

    /**
     * 人工升级 API 的 term 跃升步长，对应配置键 {@code promote.term.leap}。
     *
     * @return 跃升步长；未配置或非法（非数字或 &lt;= 0）时取接口缺省 100
     */
    @Override
    public long getPromoteTermLeap() {
        String value = properties.getProperty("promote.term.leap");
        if (value != null) {
            try {
                long leap = Long.parseLong(value.trim());
                if (leap > 0) {
                    return leap;
                }
                logger.warn("Invalid promote.term.leap value: {} (must be > 0), using default 100", value);
            } catch (NumberFormatException e) {
                logger.warn("Invalid promote.term.leap value: {}, using default 100", value);
            }
        }
        return SpeedboatConfigProvider.super.getPromoteTermLeap();
    }


    @Override
    public String getRaftPersistenceType() {
        String v = properties.getProperty("raft.persistence");
        return v == null || v.isEmpty() ? "mmap" : v.trim();
    }

    @Override
    public String getRaftPersistenceDir() {
        String v = properties.getProperty("raft.persistence.dir");
        return v == null || v.isEmpty() ? "speedboat-data" : v.trim();
    }

    @Override
    public String getRaftMmapFileName() {
        String v = properties.getProperty("raft.mmap.file.name");
        return v == null || v.isEmpty() ? "raft.mmap" : v.trim();
    }

    @Override
    public int getRaftMmapSizeMb() {
        String v = properties.getProperty("raft.mmap.size.mb");
        if (v == null || v.isEmpty()) {
            return 128;
        }
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            logger.warn("Invalid raft.mmap.size.mb='{}', fallback 128", v);
            return 128;
        }
    }

    @Override
    public int getRaftCheckpointInterval() {
        String v = properties.getProperty("raft.checkpoint.interval");
        if (v == null || v.isEmpty()) {
            return 1024;
        }
        try {
            int parsed = Integer.parseInt(v.trim());
            return parsed > 0 ? parsed : 1024;
        } catch (NumberFormatException e) {
            logger.warn("Invalid raft.checkpoint.interval='{}', fallback 1024", v);
            return 1024;
        }
    }

    @Override
    public long getLockLeaseMs() {
        String v = properties.getProperty("lock.lease.ms");
        if (v == null || v.isEmpty()) {
            return 30000L;
        }
        try {
            long parsed = Long.parseLong(v.trim());
            return parsed > 0 ? parsed : 30000L;
        } catch (NumberFormatException e) {
            logger.warn("Invalid lock.lease.ms='{}', fallback 30000", v);
            return 30000L;
        }
    }
}
