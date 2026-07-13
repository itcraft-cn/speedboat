package cn.itcraft.speedboat.lock;

public interface LockHandle extends AutoCloseable {

    boolean isSuccess();

    String getLockName();

    String getNodeId();

    @Override
    void close();
}