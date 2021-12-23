package org.apache.rocketmq.common;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@NoArgsConstructor
public abstract class ServiceThread implements Runnable {

    private static final long JOIN_TIME = 90 * 1000;

    /**
     * 真正执行任务的线程
     */
    private Thread thread;

    protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);

    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);

    /**
     * 当前任务是否停止
     */
    @Getter
    protected volatile boolean stopped = false;

    /**
     * 当前任务是否使用守护线程
     */
    @Setter
    @Getter
    protected boolean isDaemon = false;

    /**
     * Make it able to restart the thread
     */
    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * 服务名称
     *
     * @return 服务名称
     */
    public abstract String getServiceName();

    /**
     * 执行任务的方法
     */
    @Override
    public abstract void run();

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        stopped = false;
        // 创建线程，并且指定线程名称
        this.thread = new Thread(this, getServiceName());
        this.thread.setDaemon(isDaemon);
        // 直接启动线程！！！
        this.thread.start();
    }

    public void shutdown() {
        this.shutdown(false);
    }

    public void shutdown(final boolean interrupt) {
        if (!started.compareAndSet(true, false)) {
            // 如果已经停止，则不做其他操作了
            return;
        }

        // 标记停止
        this.stopped = true;

        if (hasNotified.compareAndSet(false, true)) {
            // 还没通知，则通知下
            waitPoint.countDown(); // notify
        }

        try {
            if (interrupt) {
                // 发出中断信号
                this.thread.interrupt();
            }

            if (!this.thread.isDaemon()) {
                //  非守护线程，则 join 一段时间
                this.thread.join(this.getJointime());
            }
        } catch (InterruptedException e) {
            // ignore
        }
    }

    public long getJointime() {
        return JOIN_TIME;
    }

    @Deprecated
    public void stop() {
        this.stop(false);
    }

    @Deprecated
    public void stop(final boolean interrupt) {
        if (!started.get()) {
            return;
        }
        this.stopped = true;

        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        if (interrupt) {
            this.thread.interrupt();
        }
    }

    public void makeStop() {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
    }

    public void wakeup() {
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }
    }

    protected void waitForRunning(long interval) {
        if (hasNotified.compareAndSet(true, false)) {
            this.onWaitEnd();
            return;
        }

        //entry to wait
        waitPoint.reset();

        try {
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // ignore
        } finally {
            hasNotified.set(false);
            this.onWaitEnd();
        }
    }

    protected void onWaitEnd() {
    }
}
