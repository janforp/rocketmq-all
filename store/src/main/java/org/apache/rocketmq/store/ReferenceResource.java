package org.apache.rocketmq.store;

import lombok.Getter;

import java.util.concurrent.atomic.AtomicLong;

public abstract class ReferenceResource {

    /**
     * 引用计数，新建的时候就是1
     * 提供查询服务{@link org.apache.rocketmq.store.MappedFile#selectMappedBuffer(int, int)}的时候 +1
     * 当=0的时候，说明该资源可以释放了，没有任何其他程序依赖他了
     */
    protected final AtomicLong refCount = new AtomicLong(1);

    // 是否存活，默认是true，调用 shutdown 之后变为 false
    @Getter
    protected volatile boolean available = true;

    // 是否清理完毕，执行完子类对象的 cleanup 之后就设置为 true
    protected volatile boolean cleanupOver = false;

    // 首次关闭时间

    /**
     * 第一次关闭资源的时间，（因为第一次关闭资源，可能会失败，比如说外部程序还依赖当前资源 refCount >0 此时在这记录初次关闭资源的时间）
     * 当之后再次关闭资源的时候，会传递一个间隔时间参数，如果 系统当前时间 - 首次关闭的时间 >= 间隔时间，则执行强制关闭
     */
    private volatile long firstShutdownTimestamp = 0;

    /**
     * 增长引用计数
     *
     * @return true增加成功
     */
    public synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                // 自增成功，返回成功00
                return true;
            } else {

                // 越界，一般不会发生
                this.refCount.getAndDecrement();
            }
        }

        // 不可用了
        return false;
    }

    /**
     * 第一次关闭资源的时间，（因为第一次关闭资源，可能会失败，比如说外部程序还依赖当前资源 refCount >0 此时在这记录初次关闭资源的时间）
     * 当之后再次关闭资源的时候，会传递一个间隔时间参数，如果 系统当前时间 - 首次关闭的时间 >= 间隔时间，则执行强制关闭
     *
     * @param intervalForcibly 强制关闭的时间间隔
     */
    public void shutdown(final long intervalForcibly) {
        if (this.available) {
            this.available = false;

            // 第一次关闭资源的时间
            this.firstShutdownTimestamp = System.currentTimeMillis();
            //引用计数 - 1，可能释放资源，也可能没有释放资源，具体要看引用计数是否为零
            this.release();
        } else if (this.getRefCount() > 0) {
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                // 强制关闭
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }

    /**
     * 引用计数 - 1
     */
    public void release() {
        long value = this.refCount.decrementAndGet();
        if (value > 0) {

            // 如果资源还不能清理，则返回
            return;
        }

        // 如果执行到这里：说明当前资源已经没有其他引用了，可以回收了
        synchronized (this) {
            this.cleanupOver = this.cleanup(value)/*回收*/;
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
