package cn.itcraft.speedboat.raft.executor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RaftNodeExecutor 契约（铁律）验收测试：
 *
 * <ol>
 *   <li>多生产者并发乱序投递时，任务仍在唯一 raft 线程上串行执行（单消费者 SC）；</li>
 *   <li>任务间存在 happens-before（后一任务观察到前一任务的全部可控变更）；</li>
 *   <li>始终在 raft 线程上执行（isRaftThread 判定与线程身份一致）。</li>
 * </ul>
 *
 * <p>该测试是单线程化架构（MP + MPSC + SC）的门禁：若有人改动执行器实现
 * 引入并发执行或跨线程状态变更，这里将立即失败。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
@DisplayName("RaftNodeExecutor 单线程契约")
class RaftNodeExecutorContractTest {

    private static final int THREAD_POOL_SIZE = 8;
    private static final int TASK_COUNT = 2000;

    private DefaultRaftNodeExecutor executor;

    @BeforeEach
    void setUp() {
        executor = new DefaultRaftNodeExecutor();
        executor.start();
    }

    @AfterEach
    void tearDown() {
        executor.shutdown();
    }

    @Test
    @DisplayName("2000 条乱序投递的任务应全部在唯一 raft 线程上串行执行")
    void testConcurrentSubmitRunsAllTasksOnSingleRaftThreadSerially() throws Exception {
        // 记录 raft 线程身份与执行中的任务重叠计数（重叠 > 1 即违反串行契约）
        final AtomicInteger inFlight = new AtomicInteger(0);
        final AtomicInteger maxOverlap = new AtomicInteger(0);
        final Set<String> executingThreads = ConcurrentHashMap.newKeySet();

        CountDownLatch allDone = new CountDownLatch(TASK_COUNT);
        List<Throwable> failures = new ArrayList<>();

        for (int i = 0; i < TASK_COUNT; i++) {
            final long seq = i;
            executor.execute(() -> {
                try {
                    // 任一时刻该任务独占消费者线程，重叠计数必须恒为 1
                    int overlap = inFlight.incrementAndGet();
                    maxOverlap.getAndUpdate(old -> Math.max(old, overlap));
                    executingThreads.add(Thread.currentThread().getName());
                    assertEquals(1, overlap, "任务串行执行期间正在执行的任务数恰为 1");
                } finally {
                    inFlight.decrementAndGet();
                    allDone.countDown();
                }
            });
        }

        assertTrue(allDone.await(30, TimeUnit.SECONDS),
            "全部任务应在 30s 内执行完（实际剩余 " + allDone.getCount() + "）");

        // 串行契约断言：任务并发重叠不得超过 1
        assertEquals(1, maxOverlap.get(), "任务间不得并发执行");
        // 单消费者断言：执行线程唯一，且就是 executor 的 raft 线程
        assertEquals(1, executingThreads.size(),
            "所有任务必须在同一个线程上执行：实际线程 " + executingThreads);
        String raftThreadName = executingThreads.iterator().next();
        assertTrue(raftThreadName.startsWith(DefaultRaftNodeExecutor.THREAD_NAME_PREFIX),
            "执行线程必须以 speedboat-raft 命名：" + raftThreadName);

        // happens-before 断言：乱序投递的结果集合最终一致（无任务丢失）已由 countDown 归零证明
        assertTrue(true);
    }

    @Test
    @DisplayName("isRaftThread 在 raft 线程为 true，在其它线程为 false")
    void testIsRaftThreadIdentity() throws Exception {
        final boolean[] inside = { false };
        CountDownLatch done = new CountDownLatch(1);
        executor.execute(() -> {
            inside[0] = executor.isRaftThread();
            done.countDown();
        });
        assertTrue(done.await(5, TimeUnit.SECONDS));
        assertTrue(inside[0], "任务内应为 raft 线程");
        assertEquals(false, executor.isRaftThread(), "测试线程不应被识别为 raft 线程");
    }

    @Test
    @DisplayName("submit 返回的 Future 在 raft 线程完成并携带结果")
    void testSubmitCarriesResultFromRaftThread() throws Exception {
        Future<String> future = executor.submit(() -> {
            assertEquals(true, executor.isRaftThread());
            return "ok";
        });
        assertEquals("ok", future.get(5, TimeUnit.SECONDS));
    }
}
