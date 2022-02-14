package org.apache.rocketmq.remoting.common;

import lombok.Getter;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * Base class for background thread
 */
public abstract class ServiceThread implements Runnable {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    private static final long JOIN_TIME = 90 * 1000;

    protected final Thread thread;

    protected volatile boolean hasNotified = false;

    @Getter
    protected volatile boolean stopped = false;

    public ServiceThread() {
        //名称有实现自己指定，模版方法设计
        String threadName = this.getServiceName();
        this.thread = new Thread(this, threadName);
    }

    /**
     * 子类自己实现
     *
     * @return 服务名称
     */
    public String getServiceName() {
        return this.getClass().getSimpleName();
    }

    ;

    @Override
    public abstract void run();

    public void start() {
        this.thread.start();
    }

    public void shutdown() {
        this.shutdown(false);
    }

    public void shutdown(final boolean interrupt) {
        this.stopped = true;
        log.info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);
        synchronized (this) {
            if (!this.hasNotified) {
                this.hasNotified = true;

                // TODO ？？？
                this.notify();
            }
        }

        try {
            if (interrupt) {
                // 中断一下执行任务的线程，至于是否响应，需要看线程自己了
                this.thread.interrupt();
            }
            long beginTime = System.currentTimeMillis();
            long joinTime = this.getJointime();
            //是主线程等待子线程的终止。也就是说主线程的代码块中，如果碰到了t.join()方法，此时主线程需要等待（阻塞），等待子线程结束了(Waits for this thread to die.),才能继续执行t.join()之后的代码块。
            this.thread.join(joinTime);
            long elapsedTime = System.currentTimeMillis() - beginTime;
            log.info("join thread " + this.getServiceName() + " elapsed time(ms) " + elapsedTime + " " + joinTime);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

    public long getJointime() {
        return JOIN_TIME;
    }

}
