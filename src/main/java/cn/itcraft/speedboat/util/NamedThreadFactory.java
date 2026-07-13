package cn.itcraft.speedboat.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

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