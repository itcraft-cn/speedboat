package cn.itcraft.speedboat.raft.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RaftNodeExecutor 的默认实现：单线程 ScheduledExecutorService。
 *
 * <p>与 MicroRaft 的 DefaultRaftNodeExecutor 一致——JDK 的单线程调度线程池
 * 在语义上就是一个"不限容量 MPSC 队列 + 单消费者"：
 * 多个生产者（Netty IO 线程、定时器、用户线程）提交任务，唯一消费线程串行排空。
 * 任务间 happens-before 由 {@link Executors#newSingleThreadScheduledExecutor()} 保证。</p>
 *
 * <p><b>注意事项：</b></p>
 * <ul>
 *   <li>提交的任务必须在有限时间内结束（契约禁止阻塞），否则 raft 线程被卡死，
 *       心跳/选举全部停摆；</li>
 *   <li>shutdown(timeoutMillis) 优雅停机：等待排队任务执行完毕后再退出；</li>
 *   <li>线程名固定前缀 "speedboat-raft"，便于日志与契约测试断言。</li>
 * </ul>
 *
 * @author speedboat
 * @since 1.1.0
 */
public class DefaultRaftNodeExecutor implements RaftNodeExecutor {

    private static final Logger logger = LoggerFactory.getLogger(DefaultRaftNodeExecutor.class);

    public static final String THREAD_NAME_PREFIX = "speedboat-raft";

    /** 优雅停机默认等待时间（毫秒） */
    public static final long DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = 3000;

    private final long shutdownTimeoutMillis;
    private final AtomicInteger threadSeq = new AtomicInteger(1);
    private volatile ExecutorService executor;
    /** raft 单消费者线程（供 isRaftThread 以线程身份精确判定） */
    private volatile Thread raftThread;

    /**
     * 构造执行器（尚未启动线程，{@link #start()} 后生效）。
     */
    public DefaultRaftNodeExecutor() {
        this(DEFAULT_SHUTDOWN_TIMEOUT_MILLIS);
    }

    /**
     * 构造执行器（自定义优雅停机等待时间）。
     *
     * @param shutdownTimeoutMillis shutdown 最大等待毫秒数
     */
    public DefaultRaftNodeExecutor(long shutdownTimeoutMillis) {
        this.shutdownTimeoutMillis = shutdownTimeoutMillis;
    }

    /**
     * 启动执行器（创建单线程调度池）。幂等。
     */
    public void start() {
        ensureStarted();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(Runnable task) {
        ensureStarted();
        executor.execute(task);
    }

    /**
     * 懒启动：首次使用时才创建 raft 单消费者线程。
     *
     * <p>允许上层忘记调用 start() 时仍可工作（此时任务在提交线程内联执行，
     * 语义等价于旧式同步调用）；一旦 start 后任务严格串行在 raft 线程上。
     * 线程的创建由 synchronized 保护，创建后 volatile 读无竞争。</p>
     */
    private synchronized void ensureStarted() {
        if (executor != null) {
            return;
        }
        executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, THREAD_NAME_PREFIX + "-" + threadSeq.getAndIncrement());
            raftThread = thread;
            return thread;
        });
    }

    @Override
    public Future<?> schedule(Runnable task, long delayMillis) {
        ensureStarted();
        return ((java.util.concurrent.ScheduledExecutorService) executor)
            .schedule(task, delayMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        ensureStarted();
        return executor.submit(task);
    }

    @Override
    public Future<?> scheduleAtFixedRate(Runnable task, long initialDelayMillis, long periodMillis) {
        ensureStarted();
        return ((java.util.concurrent.ScheduledExecutorService) executor)
            .scheduleAtFixedRate(task, initialDelayMillis, periodMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean isRaftThread() {
        return raftThread != null && Thread.currentThread() == raftThread;
    }

    /**
     * 优雅停机：停止接收新任务，等待已提交任务完成。
     */
    @Override
    public void shutdown() {
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(shutdownTimeoutMillis, TimeUnit.MILLISECONDS)) {
                logger.warn("Raft executor did not terminate in {} ms, forcing", shutdownTimeoutMillis);
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }
}
