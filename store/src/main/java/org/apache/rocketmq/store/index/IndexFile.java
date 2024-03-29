package org.apache.rocketmq.store.index;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.List;

/**
 * ../store/index/....
 * 文件命名：当前时间
 *
 * <a href="https://www.jianshu.com/p/e0befd11aee0">文档</a>
 *
 * 这个数据结构的设计很经典！！！！
 */
@SuppressWarnings("all")
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
     * 该文件共能存储多少个 索引，默认 2000w,可以修改配置
     */
    private final int indexNum; /*僧多*/

    /**
     * 该文件共能存储多少个 hash 槽位，默认500w,可以修改配置
     */
    private final int hashSlotNum;/*肉少*/

    // 文件
    private final MappedFile mappedFile;

    // 从 mf 中获取的内存映射缓冲区

    /**
     * 物理文件中的数据映射
     * this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
     *
     * @see IndexFile#IndexFile(java.lang.String, int, int, long, long)
     */
    private final MappedByteBuffer mappedByteBuffer;

    /**
     * 头40个字节的对象
     *
     * 8k(beginTimestamp) --- 8k(endTimestamp) --- 8k(beginPhyOffset) --- 8k(endPhyOffset) --- 4k(hashSlotCount) -- 4k(indexCount)
     */
    private final IndexHeader indexHeader;

    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum, final long endPhyOffset/*上个索引文件最后一条消息的物理偏移量*/, final long endTimestamp/*上个索引文件最后一条消息的存储时间*/) throws IOException {
        // 一个 indexFile 对象对应文件的大小
        // 40 + 500w * 4 + 2000w * 4
        int fileTotalSize = IndexHeader.INDEX_HEADER_SIZE /*40*/ + (hashSlotNum * hashSlotSize /*500w * 4*/) + (indexNum * indexSize /*2000w * 4*/);

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
            //  从 内存中写入到 buffer
            this.indexHeader.updateByteBuffer();
            // 落盘
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        // 索引条目是否使用完！！！！！
        return this.indexHeader.getIndexCount() >= this.indexNum/* 该文件共能存储多少个 索引，默认 2000w,可以修改配置*/;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * 追加索引
     *
     * @param key 需要创建索引的 key （unique_key, keys = "aaa bbb ccc" 会分别给 unique_key,aaa,bbb,ccc创建索引）
     * @param phyOffset 消息的偏移量
     * @param storeTimestamp 消息的存储时间
     * @return 成功失败？
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        if (this.indexHeader.getIndexCount() < this.indexNum) { // 当前索引文件还没满
            int keyHash/*计数得到 hash 值（正数）*/ = indexKeyHashMethod(key);
            int slotPos /*取模得到 key 对应 hash槽位 下标（注意：该slot上可能已经存储了另外一个索引的位置）*/ = keyHash % this.hashSlotNum/*该文件共能存储多少个 hash 槽位，默认500w*/;
            int absSlotPos/*计数出槽位的开始位置（绝对位置 = 40 + (pos * 4)）*/ = IndexHeader.INDEX_HEADER_SIZE/*文件头40个字节*/ + slotPos/*当前槽位*/ * hashSlotSize/*每个hash桶的大小*/;
            try {
                // 槽位上的原值，当hash冲突的时候原值是有值的，其他情况就是0
                // 其实就是索引的位置（从下标开始往后4个字节取一个整数）
                int slotOldValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotOldValue <= invalidIndex/*0*/ || slotOldValue > this.indexHeader.getIndexCount()/*该文件共能存储多少个 索引，默认 2000w*/) {
                    // 说明 slotOldValue 是无效值
                    slotOldValue = invalidIndex;
                }
                long timeDiff = storeTimestamp/*当前消息的存储时间*/ - this.indexHeader.getBeginTimestamp()/*当前索引文件第一条消息的存储时间的差*/;
                timeDiff/*转换为s（8 字节转换为 4字节）*/ = timeDiff / 1000;
                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    // 第一条索引插入的时候 timeDiff 设置为 0
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }
                /*
                 * key hash value: message key的hash值（key = topic + “#” + KEY，然后针对 key 计算 hashcode）
                 * phyOffset: message在CommitLog的物理文件地址, 可以直接查询到该消息(索引的核心机制)
                 * timeDiff: message的落盘时间与header里的beginTimestamp的差值(为了节省存储空间，如果直接存message的落盘时间就得8bytes)
                 * prevIndex: hash冲突处理的关键之处, 相同hash值上一个消息索引的index(如果当前消息索引是该hash值的第一个索引，则prevIndex=0, 也是消息索引查找时的停止条件)，每个slot位置的第一个消息的prevIndex就是0的。
                 */
                // 索引条目写入的开始位置 = 40（header） + （500w * 4） + (索引下标 * 20)
                int absIndexPos/*索引条目写入的开始位置*/ =
                        IndexHeader.INDEX_HEADER_SIZE/*文件头40个字节*/ + this.hashSlotNum/*该文件共能存储多少个 hash 槽位，默认500w*/ * hashSlotSize/*每个hash桶的大小 4*/ + this.indexHeader.getIndexCount()/*索引编号*/ * indexSize/*每个index条目的大小 20*/;

                // 下面是分别写入 20 个字节的索引的步骤

                // 索引前4个字节：存储 hash 值
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);//4
                // 索引的第 5-12个字节：存储消息的偏移量
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);//8
                // 索引的13-16个字节：存储时间差
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);//4
                // hash 桶的原值，当hash冲突的时候会使用到
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotOldValue/*hash冲突*/);//4

                /*
                 * 向当前key计数出来的 hash 桶内写入索引的编号！！！！！！
                 *
                 * 意思就是绝对偏移量是 absSlotPos 的槽位(slot) 存储的索引数据在 this.indexHeader.getIndexCount() 这个位置（共20个字节）！！！！！！
                 */
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                if (this.indexHeader.getIndexCount() <= 1/*这个索引是该索引文件的第一条索引*/) {
                    // 如果当前是第一条插入的索引，则执行下面的事情
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                // 下面是更新 header 的逻辑

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
     *
     * @param phyOffsets 里面用于存储查询结果！！！！！！！
     * @param key 查询的 key
     * @param maxNum 查询结果的最大数量
     */
    public void selectPhyOffset(final List<Long> phyOffsets/*用户存储重新结果*/, final String key, final int maxNum/*查询结果的最大数量*/, final long begin/*起始时间*/, final long end/*结束时间*/, boolean lock) {
        if (this.mappedFile.hold()/*引用计数 + 1，查询期间 mf 资源不能释放*/) {
            // 获取当前key的hash值，正数
            int keyHash = indexKeyHashMethod(key);
            // 计算出key对应的hash桶下标
            int slotPos = keyHash % this.hashSlotNum;
            // 算出 hash 桶存储的开始位置： 40 + 下标 * 4
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            try {
                // 获取槽位上的值,这个值可能是 无效值 也可能是索引编号
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount() || this.indexHeader.getIndexCount() <= 1) {
                    // 查询没有命中，无效值
                    // empty
                } else {
                    for (int nextIndexToRead/*下一个要读的索引编号，因为可能有hash冲突*/ = slotValue; /*空*/ ; /*空*/) {
                        if (phyOffsets.size() >= maxNum) {
                            // 查询达到了上限，则停止查询
                            break;
                        }

                        // 计数当前索引编号的开始位置
                        int absIndexPos = IndexHeader.INDEX_HEADER_SIZE/*40*/ + this.hashSlotNum/*默认500w*/ * hashSlotSize/*4*/ + nextIndexToRead/*索引编号*/ * indexSize/*20*/;

                        /*
                         * key hash value: message key的hash值（key = topic + “#” + KEY，然后针对 key 计算 hashcode）
                         * phyOffset: message在CommitLog的物理文件地址, 可以直接查询到该消息(索引的核心机制)
                         * timeDiff: message的落盘时间与header里的beginTimestamp的差值(为了节省存储空间，如果直接存message的落盘时间就得8bytes)
                         * prevIndex: hash冲突处理的关键之处, 相同hash值上一个消息索引的index(如果当前消息索引是该hash值的第一个索引，则prevIndex=0, 也是消息索引查找时的停止条件)，每个slot位置的第一个消息的prevIndex就是0的。
                         */

                        // 读取索引中存储的数据
                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);
                        // 转换成 ms
                        long timeDiff = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead /* hash 冲突的时候保存上一个索引下标 */ = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);
                        if (timeDiff < 0) {
                            break;
                        }
                        // 转 ms
                        timeDiff *= 1000L;
                        long timeRead/*计算出 msg 的准确的存储时间*/ = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched/*时间范围匹配*/ = (timeRead >= begin) && (timeRead <= end);
                        // 如果hash 匹配并且 时间匹配，则命中
                        if (keyHash == keyHashRead/*hash匹配*/ && timeMatched/*时间范围匹配*/) {
                            // 将结果添加到结果集合中
                            phyOffsets.add(phyOffsetRead/*结果物理偏移量*/);
                        }
                        if (prevIndexRead <= invalidIndex || prevIndexRead > this.indexHeader.getIndexCount() || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            // 判断 hash 冲突的时候保存上一个索引下标 条目的前驱索引的编号是否无效，无效则跳出查询
                            break;
                        }
                        // 如果有效则继续向前查询，有 冲突，继续查询下一个索引
                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {

                // 引用计数 - 1
                this.mappedFile.release();
            }
        }
    }
}
