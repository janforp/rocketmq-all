package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.List;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

/**
 * ../store/index/....
 * 文件命名：当前时间
 *
 * 索引文件的
 * 1.前40个字节是头
 * 2.
 * 3.
 * 4.
 * 5.
 * 6.
 */
public class IndexFile {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // 每个hash桶的大小
    private static final int hashSlotSize = 4;

    // 每个index条目的大小
    private static final int indexSize = 20;

    // 第0个索引槽是没用的
    private static final int invalidIndex = 0;

    ///// static

    /**
     * 该文件共能存储多少个 hash 槽位，默认500w,可以修改配置
     */
    private final int hashSlotNum;

    /**
     * 该文件共能存储多少个 索引，默认 2000w,可以修改配置
     */
    private final int indexNum;

    // 文件
    private final MappedFile mappedFile;

    // 从 mf 中获取的内存映射缓冲区
    private final MappedByteBuffer mappedByteBuffer;

    /**
     * 头40个字节的对象
     */
    private final IndexHeader indexHeader;

    /**
     * @param endPhyOffset 上个索引文件最后一条消息的物理偏移量
     * @param endTimestamp 上个索引文件最后一条消息的存储时间
     */
    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum, final long endPhyOffset, final long endTimestamp) throws IOException {
        // 一个 indexFile 对象对应文件的大小
        // 40 + 500w * 4 + 2000w * 4
        int fileTotalSize = IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);

        // 创建文件
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        // 该文件共能存储多少个 hash 槽位
        this.hashSlotNum = hashSlotNum;
        // 该文件共能存储多少个 索引
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        // 根据切片创建索引头
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            // 赋值，上一个文件的最后
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            // 赋值，上一个文件的最后
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public void load() {
        this.indexHeader.load();
    }

    // 落盘
    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            // 更新头
            this.indexHeader.updateByteBuffer();
            // 落盘
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        // 索引条目是否使用完！！！！！
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * 追加索引
     *
     * @param key 需要创建索引的 key
     * @param phyOffset 消息的偏移量
     * @param storeTimestamp 消息的存储时间
     * @return 成功失败？
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        if (this.indexHeader.getIndexCount() < this.indexNum) { // 当前索引文件还没满

            // 计数得到 hash 值
            int keyHash = indexKeyHashMethod(key);
            // 计数槽位 下标
            int slotPos = keyHash % this.hashSlotNum;
            // 绝对位置 = 40 + (pos * 4)
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;
            try {
                // 槽位上的数据
                // 其实就是索引的位置（从下标开始往后4个字节取一个整数）
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // TODO ？
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }

                // 首条消息的存储时间 - 当前消息的存储时间
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();
                // 转换为s
                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }

                // 索引下标 = 40 + （500w * 4） + (索引下标 * 20)
                int absIndexPos = IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize + this.indexHeader.getIndexCount() * indexSize;

                // 索引前4个字节：存储 hash 值
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                // 索引的第 5-12个字节：存储消息的偏移量
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                // 索引的13-16个字节：存储时间差
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                // TODO 索引的17-20个字节：存储下标？
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);

                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                // 索引的下标槽位数量 +1
                this.indexHeader.incHashSlotCount();
                // 使用的索引数量 + 1
                this.indexHeader.incIndexCount();
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            }
        } else {
            // 当前索引文件已经满了！！！！！！！！
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount() + "; index max num = " + this.indexNum);
        }

        return false;
    }

    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0) {
            keyHashPositive = 0;
        }
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }

    /**
     * 查询
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum, final long begin, final long end, boolean lock) {
        if (this.mappedFile.hold()) {
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            try {
                // 获取槽位上的值
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount() || this.indexHeader.getIndexCount() <= 1) {
                    // empty
                } else {
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }

                        int absIndexPos = IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize + nextIndexToRead * indexSize;
                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);
                        long timeDiff = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);
                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;

                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        if (prevIndexRead <= invalidIndex || prevIndexRead > this.indexHeader.getIndexCount() || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                this.mappedFile.release();
            }
        }
    }
}
