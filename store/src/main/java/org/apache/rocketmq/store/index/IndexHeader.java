package org.apache.rocketmq.store.index;

import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * IndexFile 的头
 * 头共40个字节
 *
 * 8k(beginTimestamp) --- 8k(endTimestamp) --- 8k(beginPhyOffset) --- 8k(endPhyOffset) --- 4k(hashSlotCount) -- 4k(indexCount)
 *
 */
@SuppressWarnings("all")
public class IndexHeader {

    public static final int INDEX_HEADER_SIZE = 40;

    /**
     * 开始时间存储从第零个字节开始，共占用8字节
     */
    private static final int beginTimestampIndex = 0;

    /**
     * 最后存储时间存储从第8个字节开始，共占用8字节
     */
    private static final int endTimestampIndex = 8;

    /**
     * 开始物理偏移量从第16个字节，共占用8字节
     */
    private static final int beginPhyoffsetIndex = 16;

    /**
     * 结束物理偏移量从第24个字节，共占用8字节
     */
    private static final int endPhyoffsetIndex = 24;

    /**
     * hashSlotcount从第32个字节，共占用4字节
     *
     * hash槽
     */
    private static final int hashSlotcountIndex = 32;

    /**
     * indexCount从第36个字节，共占用4字节
     */
    private static final int indexCountIndex = 36;

    /**
     * 共40个字节的缓冲
     */
    private final ByteBuffer byteBuffer;

    /**
     * beginTimestamp : 该索引文件的第一个消息(Message)的存储时间(落盘时间) 物理位置(pos: 0-7) 8bytes
     */
    private final AtomicLong beginTimestamp = new AtomicLong(0);

    /**
     * endTimestamp : 该索引文件的最后一个消息(Message)的存储时间(落盘时间) 物理位置(pos: 8-15) 8bytes
     */
    private final AtomicLong endTimestamp = new AtomicLong(0);

    /**
     * beginPhyoffset : 该索引文件第一个消息(Message)的在CommitLog(消息存储文件)的物理位置偏移量(可以通过该物理偏移直接获取到该消息) 物理位置(pos: 16-23) 8bytes
     */
    private final AtomicLong beginPhyOffset = new AtomicLong(0);

    /**
     * endPhyoffset : 该索引文件最后一个消息(Message)的在CommitLog(消息存储文件)的物理位置偏移量 (pos: 24-31) 8bytes
     */
    private final AtomicLong endPhyOffset = new AtomicLong(0);

    /**
     * 槽位数
     */
    @Getter
    private final AtomicInteger hashSlotCount = new AtomicInteger(0);

    /**
     * 记录该文件当前使用的索引个数
     * 也就是下一个索引创建的时候使用的索引
     *
     * indexCount : 该索引文件目前的索引个数 (pos: 36-39) 4bytes
     */
    private final AtomicInteger indexCount = new AtomicInteger(1/*从1开始，因为第0个是无效第索引*/);

    public IndexHeader(final ByteBuffer byteBuffer/* ByteBuffer byteBuffer = this.mappedByteBuffer.slice(); 其实是从文件中来*/) {
        this.byteBuffer = byteBuffer;
    }

    public void load() {
        // 把 buffer 中的数据赋值给对象

        this.beginTimestamp.set(byteBuffer.getLong(beginTimestampIndex));
        this.endTimestamp.set(byteBuffer.getLong(endTimestampIndex));
        this.beginPhyOffset.set(byteBuffer.getLong(beginPhyoffsetIndex));
        this.endPhyOffset.set(byteBuffer.getLong(endPhyoffsetIndex));

        this.hashSlotCount.set(byteBuffer.getInt(hashSlotcountIndex));
        this.indexCount.set(byteBuffer.getInt(indexCountIndex));

        if (this.indexCount.get() <= 0) {
            this.indexCount.set(1/*从1开始，因为第0个是无效第索引*/);
        }
    }

    public void updateByteBuffer() {
        // 从 内存中写入到 buffer
        this.byteBuffer.putLong(beginTimestampIndex, this.beginTimestamp.get());
        this.byteBuffer.putLong(endTimestampIndex, this.endTimestamp.get());
        this.byteBuffer.putLong(beginPhyoffsetIndex, this.beginPhyOffset.get());
        this.byteBuffer.putLong(endPhyoffsetIndex, this.endPhyOffset.get());
        this.byteBuffer.putInt(hashSlotcountIndex, this.hashSlotCount.get());
        this.byteBuffer.putInt(indexCountIndex, this.indexCount.get());
    }

    public long getBeginTimestamp() {
        return beginTimestamp.get();
    }

    public void setBeginTimestamp(long beginTimestamp) {
        this.beginTimestamp.set(beginTimestamp);
        this.byteBuffer.putLong(beginTimestampIndex, beginTimestamp);
    }

    public long getEndTimestamp() {
        return endTimestamp.get();
    }

    public void setEndTimestamp(long endTimestamp) {
        this.endTimestamp.set(endTimestamp);
        this.byteBuffer.putLong(endTimestampIndex, endTimestamp);
    }

    public long getBeginPhyOffset() {
        return beginPhyOffset.get();
    }

    public void setBeginPhyOffset(long beginPhyOffset) {
        this.beginPhyOffset.set(beginPhyOffset);
        this.byteBuffer.putLong(beginPhyoffsetIndex, beginPhyOffset);
    }

    public long getEndPhyOffset() {
        return endPhyOffset.get();
    }

    public void setEndPhyOffset(long endPhyOffset) {
        this.endPhyOffset.set(endPhyOffset);
        this.byteBuffer.putLong(endPhyoffsetIndex, endPhyOffset);
    }

    public void incHashSlotCount() {
        int value = this.hashSlotCount.incrementAndGet();
        this.byteBuffer.putInt(hashSlotcountIndex, value);
    }

    public int getIndexCount() {
        return indexCount.get();
    }

    public void incIndexCount() {
        int value = this.indexCount.incrementAndGet();
        this.byteBuffer.putInt(indexCountIndex, value);
    }
}
