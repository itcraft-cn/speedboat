package cn.itcraft.speedboat.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 命名线程工厂，为 Speedboat 组件创建具有可识别名称的线程。
 * 
 * <p>在线程池和异步任务中提供清晰的线程命名，便于监控、调试和日志追踪。
 * 支持守护线程和非守护线程模式。</p>
 * 
 * <p>线程名称格式：Speedboat-{component}-{nodeId}-{sequence}</p>
 * <ul>
 *   <li>component: 组件名称（如 "RaftNode"、"NettyTransport"）</li>
 *   <li>nodeId: 节点标识</li>
 *   <li>sequence: 自增序列号</li>
 * </ul>
 * 
 * <p>使用示例：</p>
 * <pre>{@code
 * // 创建守护线程工厂
 * ThreadFactory factory = NamedThreadFactory.forComponent("RaftNode", "node-1");
 * ExecutorService executor = Executors.newFixedThreadPool(4, factory);
 * 
 * // 创建非守护线程工厂
 * ThreadFactory nonDaemonFactory = NamedThreadFactory.forComponentNonDaemon("Lock", "node-2");
 * }</pre>
 * 
 * @author speedboat
 * @see ThreadFactory
 * @since 1.0.0
 */
public class NamedThreadFactory implements ThreadFactory {

    private final String namePrefix;
    private final AtomicInteger counter = new AtomicInteger(1);
    private final boolean daemon;

    public NamedThreadFactory(String component, String nodeId, boolean daemon) {
        this.namePrefix = "Speedboat-" + component + "-" + nodeId;
        this.daemon = daemon;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, namePrefix + "-" + counter.getAndIncrement());
        thread.setDaemon(daemon);
        return thread;
    }

    public static NamedThreadFactory forComponent(String component, String nodeId) {
        return new NamedThreadFactory(component, nodeId, true);
    }

    public static NamedThreadFactory forComponentNonDaemon(String component, String nodeId) {
        return new NamedThreadFactory(component, nodeId, false);
    }
}