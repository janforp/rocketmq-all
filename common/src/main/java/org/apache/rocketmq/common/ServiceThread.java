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

    /**
     * 基于 AQS 实现的，可以重置的 门闩
     *
     * @see java.util.concurrent.CountDownLatch
     */
    protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);

    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);

    /**
     * 当前任务是否停止
     */
    @Getter
    protected volatile boolean stopped = false;

    /**
     * 当前任务{@link org.apache.rocketmq.common.ServiceThread#thread}是否使用守护线程
     */
    @Setter
    @Getter
    protected boolean isDaemon = false;

    /**
     * Make it able to restart the thread
     * 当前对象的任务是否开始，可以通过该字段，重复开始该对象中的任务，不同的是每次调用 {@link  ServiceThread#start() } 方法都会新建一个线程去执行任务
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

    /**
     * 创建线程，并且启动线程，开始执行 run 方法
     *
     * 当前对象可以通过控制 {@link ServiceThread#started} 的开工，重复调用 start 方法，每次都会重新创建一个线程，达到复用当前对象的目的
     */
    public void start() {

        // AtomicBoolean started = new AtomicBoolean(false);
        // 设置标志为已经开始
        if (!started.compareAndSet(false, true)) {
            return;
        }
        stopped = false;
        /*
         * 创建线程，并且指定线程名称
         * @see ServiceThread#run()
         */
        this.thread = new Thread(this /*线程启动的时候就会执行当前对象的 run 方法，该方法留给子类去实现*/, getServiceName());
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
            // 如果还没开启，则啥也不走
            return;
        }

        // 标记停止
        this.stopped = true;

        if (hasNotified.compareAndSet(false, true)) {
            // 如果没通知过就通知一下
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
