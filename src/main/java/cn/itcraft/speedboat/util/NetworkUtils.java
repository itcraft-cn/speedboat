package cn.itcraft.speedboat.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.Random;

/**
 * 网络工具类
 * 
 * <p>提供自动检测 hostname、IP 和生成 nodeId 的功能</p>
 *
 * @author speedboat
 * @since 1.0.0
 */
public class NetworkUtils {
    
    private static final Logger logger = LoggerFactory.getLogger(NetworkUtils.class);
    
    private static final Random RANDOM = new Random();
    
    /**
     * 检测本机 hostname
     *
     * @return hostname，检测失败返回 "unknown"
     */
    public static String detectHostname() {
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            logger.debug("Detected hostname: {}", hostname);
            return hostname;
        } catch (Exception e) {
            logger.warn("Failed to detect hostname, using 'unknown'", e);
            return "unknown";
        }
    }
    
    /**
     * 检测本机非回环 IP 地址
     * 
     * <p>优先返回：</p>
     * <ul>
     *   <li>非回环、非链路本地、已激活的 IPv4 地址</li>
     *   <li>如果有多网卡，返回第一个符合条件的</li>
     * </ul>
     *
     * @return IP 地址，检测失败返回 "127.0.0.1"
     */
    public static String detectLocalIp() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces != null && interfaces.hasMoreElements()) {
                NetworkInterface ni = interfaces.nextElement();
                if (!ni.isUp() || ni.isLoopback() || ni.isVirtual()) {
                    continue;
                }
                
                Enumeration<InetAddress> addresses = ni.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress addr = addresses.nextElement();
                    if (addr.isLoopbackAddress() || addr.isLinkLocalAddress()) {
                        continue;
                    }
                    
                    String ip = addr.getHostAddress();
                    if (ip != null && !ip.isEmpty()) {
                        logger.debug("Detected local IP: {} (interface: {})", ip, ni.getName());
                        return ip;
                    }
                }
            }
        } catch (SocketException e) {
            logger.warn("Failed to detect local IP, using fallback", e);
        }
        
        try {
            String fallbackIp = InetAddress.getLocalHost().getHostAddress();
            logger.debug("Using fallback IP: {}", fallbackIp);
            return fallbackIp;
        } catch (Exception e) {
            logger.warn("Failed to get fallback IP, using 127.0.0.1", e);
            return "127.0.0.1";
        }
    }
    
    /**
     * 自动生成 nodeId
     *
     * <p>格式：hostname-ip-random4位</p>
     * <p>示例：server01-192.168.10.1-0012</p>
     *
     * @return nodeId
     * @deprecated 随机后缀会导致 Raft 身份跨重启漂移（votedFor/leaderId 失稳）。
     *             生产路径请使用确定性生成 {@link #generateNodeId(String address)}。
     */
    @Deprecated
    public static String generateNodeId() {
        String hostname = detectHostname();
        String ip = detectLocalIp();
        String random = String.format("%04d", RANDOM.nextInt(10000));
        String nodeId = hostname + "-" + ip + "-" + random;
        logger.info("Generated nodeId: {}", nodeId);
        return nodeId;
    }
    
    /**
     * 根据 hostname 和 IP 生成 nodeId
     *
     * <p>身份确定性要求：同一进程多次运行必须生成相同的 nodeId，否则会破坏 Raft
     * 的安全前提（votedFor/leaderId/成员身份的稳定性）。禁止在常规身份中掺入随机后缀。</p>
     *
     * @param hostname 主机名
     * @param ip IP 地址
     * @return nodeId
     */
    public static String generateNodeId(String hostname, String ip) {
        return hostname + "-" + ip;
    }

    /**
     * 根据地址（ip:port）生成确定性 nodeId。
     *
     * <p>格式：{@code node-<ip>-<port>}。这是自节点与 peer 节点生成身份的
     * 统一入口——任何进程对同一个 {@code ip:port} 都能推导出完全一致的
     * nodeId，保证集群内一个节点只有一个身份。优先使用本方法，
     * 而不是 {@link #generateNodeId(String, String)}（peers 侧无法获知 hostname）。</p>
     *
     * @param address 节点地址（格式 "ip:port"）
     * @return 确定性 nodeId
     */
    public static String generateNodeId(String address) {
        String ip = parseIp(address);
        int port = parsePort(address);
        return "node-" + ip + "-" + port;
    }

    /**
     * 从地址字符串解析端口
     * 
     * <p>格式："ip:port"</p>
     *
     * @param address 地址字符串
     * @return 端口号
     */
    public static int parsePort(String address) {
        if (address == null || !address.contains(":")) {
            throw new IllegalArgumentException("Invalid address format: " + address);
        }
        
        String[] parts = address.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid address format: " + address);
        }
        
        try {
            return Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid port in address: " + address, e);
        }
    }
    
    /**
     * 从地址字符串解析 IP
     *
     * @param address 地址字符串
     * @return IP 地址
     */
    public static String parseIp(String address) {
        if (address == null || !address.contains(":")) {
            throw new IllegalArgumentException("Invalid address format: " + address);
        }
        
        String[] parts = address.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid address format: " + address);
        }
        
        return parts[0];
    }
    
    /**
     * 判断 IP 是否匹配地址
     *
     * @param ip IP 地址
     * @param address 地址字符串（ip:port）
     * @return true 表示匹配
     */
    public static boolean ipMatchesAddress(String ip, String address) {
        if (ip == null || address == null) {
            return false;
        }
        
        String addressIp = parseIp(address);
        return ip.equals(addressIp);
    }
}