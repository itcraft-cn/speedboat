package cn.itcraft.speedboat.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.itcraft.speedboat.config.SpeedboatConsts;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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
    public cn.itcraft.speedboat.strategy.voteweight.VoteWeightStrategy getVoteWeightStrategy() {
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
            return new cn.itcraft.speedboat.strategy.voteweight.PreferNodeVoteWeightStrategy(prefer.trim(), extra);
        }
        if ("even".equals(modeVal)) {
            return new cn.itcraft.speedboat.strategy.voteweight.EvenNodeVoteWeightStrategy();
        }
        throw new IllegalArgumentException("Unknown vote.weight.strategy: " + modeVal);
    }

    @Override
    public cn.itcraft.speedboat.config.MembershipConfig getMembershipConfig() {
        String autoRemoval = properties.getProperty("membership.auto.removal");
        if (autoRemoval == null) {
            return new cn.itcraft.speedboat.config.MembershipConfig();
        }
        boolean enable = Boolean.parseBoolean(autoRemoval.trim());
        cn.itcraft.speedboat.config.MembershipConfig def = new cn.itcraft.speedboat.config.MembershipConfig();
        return new cn.itcraft.speedboat.config.MembershipConfig(
            def.getHealthCheckIntervalMs(), def.getFailureThreshold(), def.getConfirmationPeriods(),
            def.getRegistryCheckIntervalMs(), def.getRegistryImpl(), enable);
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
