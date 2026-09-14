package cn.itcraft.speedboat.raft.executor;

/**
 * Raft 内部任务执行器契约（对标 MicroRaft io.microraft.executor.RaftNodeExecutor）。
 *
 * <p><b>核心契约（铁律）：</b></p>
 * <ol>
 *   <li>同一时刻最多只有 1 个任务在执行（单消费者 SC）；</li>
 *   <li>任意两个任务的执行之间存在 happens-before 关系（消息生产者 MP 经受限
 *       MPSC 队列递交任务，消费者线程串行排空）；</li>
 *   <li>提交到本执行器的任务<b>绝不允许阻塞</b>：不得发起同步建连、同步磁盘 IO、
 *       不得等待其它线程或 Future（会饿死消费者线程）；</li>
 *   <li>状态机角色 / 任期 / 日志 / 成员 / commitIndex 等结构变更只允许在
 *       {@link #isRaftThread()} 为 true 的 raft 消费线程上发生；
 *       其它线程仅可提交任务或读取 volatile 快照。</li>
 * </ol>
 *
 * <p>该契约成立后，Raft 算法本体（状态机、消息处理器）内部无需任何
 * {@code synchronized} / 锁，与 MicroRaft "零锁零阻塞" 的做法一致。</p>
 *
 * @author speedboat
 * @see DefaultRaftNodeExecutor
 * @since 1.1.0
 */
public interface RaftNodeExecutor {

    /**
     * 启动执行器（创建 raft 单消费者线程）。幂等。
     */
    void start();

    /**
     * 立即提交一个任务到 MPSC 队列（非阻塞，快速返回）。
     *
     * <p>调用线程任意；任务将在 raft 单消费者线程上串行执行。</p>
     *
     * @param task 待执行任务，不允许为 null
     */
    void execute(Runnable task);

    /**
     * 在指定延迟后在 raft 线程上调度执行一个任务。
     *
     * <p>用于心跳 tick、选举超时重试、成员变更检测等周期任务；
     * 周期任务应通过任务内部自续期（在任务执行末尾再次 schedule 自身）实现，
     * 便于根据角色/运行状态动态取消。</p>
     *
     * @param task        待执行任务
     * @param delayMillis 延迟毫秒数，必须 >= 0
     * @return 可用于取消的 Future（仅取消用途，返回值无意义）
     */
    java.util.concurrent.Future<?> schedule(Runnable task, long delayMillis);

    /**
     * 提交任务并返回 Future，供外部线程"投递 raft 任务并等待结果"使用。
     *
     * <p>主要用于兼容旧同步 API（如 {@code propose} 返回日志索引的既有签名）：
     * 外部线程提交任务后在自身线程上 {@code Future#get} 阻塞等待，
     * raft 线程不被阻塞。</p>
     *
     * @param task 待执行任务
     * @param <T>  返回类型
     * @return Future，在 raft 线程执行完成后完成
     */
    <T> java.util.concurrent.Future<T> submit(java.util.concurrent.Callable<T> task);

    /**
     * 在 raft 线程上以固定频率执行周期任务（自首次延时后，每 period 周期一次）。
     *
     * <p>用于心跳 tick、成员健康检测等固定节拍任务；
     * 选举超时等需要"重置"语义的用 {@link #schedule(Runnable, long)}。</p>
     *
     * @param task              周期任务
     * @param initialDelayMillis 首次执行延迟毫秒数
     * @param periodMillis      固定周期毫秒数
     * @return 可用于取消的 Future
     */
    java.util.concurrent.Future<?> scheduleAtFixedRate(Runnable task, long initialDelayMillis, long periodMillis);

    /**
     * 判断当前线程是否就是 raft 单消费者线程。
     *
     * <p>RaftNode 的内部私有方法应优先信任调用链（均由本执行器调度），
     * 但对外兼容的同步门面方法需要用它实现"已在 raft 线程则直接内联执行，
     * 否则投递等待"的防重入逻辑。</p>
     *
     * @return 当前线程是否为 raft 消费线程
     */
    boolean isRaftThread();

    /**
     * 关闭执行器。幂等；调用后不再接受新任务，已提交任务尽量执行完（优雅停机）。
     */
    void shutdown();
}
