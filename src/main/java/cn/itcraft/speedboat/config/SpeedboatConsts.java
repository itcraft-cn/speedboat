package cn.itcraft.speedboat.config;

/**
 * Speedboat 常量定义类
 * 
 * <p>定义 Raft 分布式锁控系统所需的核心常量，包括：</p>
 * <ul>
 *   <li>选举超时时间参数</li>
 *   <li>心跳间隔参数</li>
 *   <li>序列化相关常量</li>
 *   <li>端口配置</li>
 * </ul>
 *
 * @author speedboat
 * @since 1.0.0
 */
public final class SpeedboatConsts {

    private SpeedboatConsts() {
    }

    public static final int MIN_ELECTION_TIMEOUT_MS = 150;
    
    public static final int MAX_ELECTION_TIMEOUT_MS = 300;
    
    public static final int HEARTBEAT_INTERVAL_MS = 50;
    
    public static final int GLOBAL_MIN_ELECTION_TIMEOUT_MS = 500;
    
    public static final int GLOBAL_MAX_ELECTION_TIMEOUT_MS = 1000;
    
    public static final int GLOBAL_HEARTBEAT_INTERVAL_MS = 200;
    
    public static final int DEFAULT_SERIALIZATION_TYPE = 2;
    
    public static final int SERIALIZATION_HEADER_LEN = 9;
    
    public static final int CRC32_LEN = 4;
    
    public static final int TYPE_LEN = 1;
    
    public static final int DEFAULT_PORT = 22222;
    
    public static final int GLOBAL_PORT = 33333;
    
    public static final int SHUTDOWN_TIMEOUT_SECONDS = 1;

    public static final int DEFAULT_MAX_LOG_SIZE = 10;
}