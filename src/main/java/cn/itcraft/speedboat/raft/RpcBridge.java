package cn.itcraft.speedboat.raft;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

/**
 * RPC 异步桥接（内部协作器，非公开 API）。
 *
 * <p>入站消息与 raft 单线程之间的标准搬运桥：调用线程拿到 Future 即返回，
 * 绝不阻塞等待接收方 raft 线程（避免 mock 集群跨节点 raft 线程互等死锁）。
 * 语义由 {@code RaftNodeImpl.onRaftThreadAsync} 实现——此处仅协议化签名，
 * 供"每 RPC 一个 Handler 类"统一注入。</p>
 *
 * @author speedboat
 * @since 1.1.0
 */
interface RpcBridge {

    /**
     * raft 线程异步执行任务，结果（含异常）写入返回的 Future。
     *
     * @param task raft 单线程上执行的任务
     * @param <T>  结果类型
     * @return 结果 Future
     */
    <T> CompletableFuture<T> submit(Callable<T> task);
}
