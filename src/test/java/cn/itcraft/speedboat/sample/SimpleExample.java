package cn.itcraft.speedboat.sample;

import cn.itcraft.speedboat.Speedboat;
import cn.itcraft.speedboat.config.PropertiesConfigProvider;
import cn.itcraft.speedboat.config.SpeedboatConfigProvider;

/**
 * Speedboat 极简使用示例
 * 
 * <p>展示最简单的使用方式：</p>
 * <ol>
 *   <li>创建 Properties 配置文件</li>
 *   <li>一行启动 Speedboat</li>
 *   <li>查询主节点状态</li>
 *   <li>一行停止</li>
 * </ol>
 *
 * <p>使用步骤：</p>
 * <pre>
 * 1. 创建 config.properties 文件（在同目录或指定路径）：
 *    nodes.0.0=192.168.10.1:3000
 *    nodes.0.1=192.168.10.2:3000
 *    nodes.0.2=192.168.10.3:3000
 * 
 * 2. 在三台机器上分别运行本示例（每台机器 IP 对应配置中的一个节点）
 * 3. 系统自动：
 *    - 检测本机 hostname 和 IP
 *    - 匹配配置中的节点
 *    - 生成 nodeId
 *    - 启动选举
 * </pre>
 *
 * @author speedboat
 * @since 1.0.0
 */
public class SimpleExample {
    
    public static void main(String[] args) throws Exception {
        System.out.println("=================================");
        System.out.println("Speedboat 极简示例");
        System.out.println("=================================");
        
        String configFile = args.length > 0 ? args[0] : "config.properties";
        
        System.out.println("配置文件: " + configFile);
        
        SpeedboatConfigProvider config = new PropertiesConfigProvider(configFile);
        
        System.out.println("启动 Speedboat...");
        Speedboat.start(config);
        
        System.out.println("节点ID: " + Speedboat.getNodeId());
        System.out.println("机房ID: " + Speedboat.getDatacenterId());
        
        System.out.println("等待选举完成...");
        Thread.sleep(3000);
        
        System.out.println("=================================");
        System.out.println("集群状态监控");
        System.out.println("=================================");
        
        Thread monitorThread = new Thread(() -> {
            while (Speedboat.isRunning()) {
                try {
                    Thread.sleep(2000);
                    
                    boolean isMain = Speedboat.isMain();
                    String leaderId = Speedboat.getLeaderId();
                    long term = Speedboat.getTerm();
                    
                    System.out.println(String.format(
                        "[%s] %s (Leader: %s, Term: %d)",
                        Speedboat.getNodeId(),
                        isMain ? "主节点" : "从节点",
                        leaderId != null ? leaderId : "unknown",
                        term
                    ));
                    
                } catch (Exception e) {
                    break;
                }
            }
        });
        monitorThread.setDaemon(true);
        monitorThread.start();
        
        System.out.println("按 Ctrl+C 停止...");
        
        try {
            while (true) {
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        System.out.println("停止 Speedboat...");
        Speedboat.stop();
        
        System.out.println("已停止");
    }
}