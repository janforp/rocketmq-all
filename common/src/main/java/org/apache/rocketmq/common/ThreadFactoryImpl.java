package org.apache.rocketmq.common;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 线程工厂
 */
public class ThreadFactoryImpl implements ThreadFactory {

    /**
     * 计数器
     */
    private final AtomicLong threadIndex = new AtomicLong(0);

    /**
     * 线程名称前缀
     */
    private final String threadNamePrefix;

    /**
     * 是否守护线程
     */
    private final boolean daemon;

    public ThreadFactoryImpl(final String threadNamePrefix) {
        this(threadNamePrefix, false);
    }

    public ThreadFactoryImpl(final String threadNamePrefix, boolean daemon) {
        this.threadNamePrefix = threadNamePrefix;
        this.daemon = daemon;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, threadNamePrefix + this.threadIndex.incrementAndGet());
        thread.setDaemon(daemon);
        return thread;
    }
}
