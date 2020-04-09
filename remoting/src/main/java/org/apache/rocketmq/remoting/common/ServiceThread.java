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

    public abstract String getServiceName();

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
                this.notify();
            }
        }

        try {
            if (interrupt) {
                this.thread.interrupt();
            }
            long beginTime = System.currentTimeMillis();
            long jointime = this.getJointime();
            //是主线程等待子线程的终止。也就是说主线程的代码块中，如果碰到了t.join()方法，此时主线程需要等待（阻塞），等待子线程结束了(Waits for this thread to die.),才能继续执行t.join()之后的代码块。
            this.thread.join(jointime);
            long elapsedTime = System.currentTimeMillis() - beginTime;
            log.info("join thread " + this.getServiceName() + " elapsed time(ms) " + elapsedTime + " " + jointime);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

    public long getJointime() {
        return JOIN_TIME;
    }
}
