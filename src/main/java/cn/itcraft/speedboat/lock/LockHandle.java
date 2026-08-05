package cn.itcraft.speedboat.lock;

/**
 * LockHandle 接口。
 * 
 * @author speedboat
 * @since 1.0.0
 */
public interface LockHandle extends AutoCloseable {

    boolean isSuccess();

    String getLockName();

    String getNodeId();

    @Override
    void close();
}