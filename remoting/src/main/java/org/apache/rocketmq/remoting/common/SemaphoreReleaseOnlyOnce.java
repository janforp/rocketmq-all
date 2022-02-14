package org.apache.rocketmq.remoting.common;

import lombok.Getter;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

// 一个对象只是否一次
public class SemaphoreReleaseOnlyOnce {

    // 一个对象只释放一次
    private final AtomicBoolean released = new AtomicBoolean(false);

    @Getter
    private final Semaphore semaphore;

    public SemaphoreReleaseOnlyOnce(Semaphore semaphore) {
        this.semaphore = semaphore;
    }

    public void release() {
        if (this.semaphore != null) {
            if (this.released.compareAndSet(false, true)) {
                this.semaphore.release();
            }
        }
    }
}