package org.apache.rocketmq.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * TransientStorePool的构造器会根据MessageStoreConfig设置poolSize、fileSize属性；其init方法会创建poolSize个byteBuffer放入到availableBuffers中；其destroy方法会遍历availableBuffers，然后取出其address进行LibC.INSTANCE.munlock
 * borrowBuffer返回availableBuffers.pollFirst()，returnBuffer方法会执行byteBuffer.position(0)以及byteBuffer.limit(fileSize)，然后offerFirst方法放入availableBuffers
 * availableBufferNums方法在storeConfig.isTransientStorePoolEnable()为true的情况下会返回availableBuffers.size()，否则返回Integer.MAX_VALUE
 */
public class TransientStorePool {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final int poolSize;

    private final int fileSize;

    // 保存申请到的 fileSize 个内容
    private final Deque<ByteBuffer> availableBuffers;

    private final MessageStoreConfig storeConfig;

    /**
     * TransientStorePool的构造器会根据MessageStoreConfig设置poolSize、fileSize属性；
     *
     * 其init方法会创建poolSize个byteBuffer放入到availableBuffers中；
     *
     * 其destroy方法会遍历availableBuffers，然后取出其address进行LibC.INSTANCE.munlock
     *
     * borrowBuffer返回availableBuffers.pollFirst()，
     *
     * returnBuffer方法会执行byteBuffer.position(0)以及byteBuffer.limit(fileSize)，然后offerFirst方法放入availableBuffers
     *
     * availableBufferNums方法在storeConfig.isTransientStorePoolEnable()为true的情况下会返回availableBuffers.size()，否则返回Integer.MAX_VALUE
     */
    public TransientStorePool(final MessageStoreConfig storeConfig) {
        this.storeConfig = storeConfig;

        // 5
        this.poolSize = storeConfig.getTransientStorePoolSize();

        // 1G
        this.fileSize = storeConfig.getMappedFileSizeCommitLog();
        this.availableBuffers = new ConcurrentLinkedDeque<>();
    }

    /**
     * It's a heavy init method.
     *
     * 其init方法会创建poolSize个byteBuffer放入到availableBuffers中；
     *
     * @see DefaultMessageStore#DefaultMessageStore(org.apache.rocketmq.store.config.MessageStoreConfig, org.apache.rocketmq.store.stats.BrokerStatsManager, org.apache.rocketmq.store.MessageArrivingListener, org.apache.rocketmq.common.BrokerConfig)
     */
    public void init() {
        for (int i = 0; i < poolSize/*5*/; i++) {

            // 申请 1G 内容
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);

            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);

            LibC libC = LibC.INSTANCE;

            // TODO ? 锁定这部分内存？
            libC.mlock(pointer, new NativeLong(fileSize));

            // 加入集合中
            availableBuffers.offer(byteBuffer);
        }
    }

    // 其destroy方法会遍历availableBuffers，然后取出其address进行LibC.INSTANCE.munlock
    public void destroy() {
        for (ByteBuffer byteBuffer : availableBuffers) {
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
        }
    }

    // returnBuffer方法会执行byteBuffer.position(0)以及byteBuffer.limit(fileSize)，然后offerFirst方法放入availableBuffers
    public void returnBuffer(ByteBuffer byteBuffer) {
        byteBuffer.position(0);
        byteBuffer.limit(fileSize);
        this.availableBuffers.offerFirst(byteBuffer);
    }

    // borrowBuffer返回availableBuffers.pollFirst()，
    public ByteBuffer borrowBuffer() {
        ByteBuffer buffer = availableBuffers.pollFirst();
        if (availableBuffers.size() < poolSize * 0.4) {
            log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
        }
        return buffer;
    }

    // availableBufferNums方法在storeConfig.isTransientStorePoolEnable()为true的情况下会返回availableBuffers.size()，否则返回Integer.MAX_VALUE
    public int availableBufferNums() {
        if (storeConfig.isTransientStorePoolEnable()) {
            return availableBuffers.size();
        }
        return Integer.MAX_VALUE;
    }
}
