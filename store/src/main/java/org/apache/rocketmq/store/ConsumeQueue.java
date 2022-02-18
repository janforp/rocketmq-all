package org.apache.rocketmq.store;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * CQData ----- consumeQueue data
 */
public class ConsumeQueue {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // 每个 CQDATA 单元的数据字节大小
    public static final int CQ_STORE_UNIT_SIZE = 20;

    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    // 存储主模块
    private final DefaultMessageStore defaultMessageStore;

    // 当前 cq 文件管理器
    private final MappedFileQueue mappedFileQueue; /* users/zhuchenjian/store/consumequeue */

    // ConsumeQueue主题
    @Getter
    private final String topic;

    // ConsumeQueue队列，每一个队列都有一个ConsumeQueue对象管理
    @Getter
    private final int queueId;

    /**
     * TODO ？？？
     * 临时缓冲区，用途：插新的 CQData 使用
     *
     * this.byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE 20);
     */
    private final ByteBuffer byteBufferIndex;

    // 每一个 ConsumeQueue 存储文件大小，默认是 600w字节
    private final int mappedFileSize;  /* 6000000 KB =  5.72 MB*/

    // 当前 ConsumeQueue 最大消息物理偏移量
    @Getter
    @Setter
    private long maxPhysicOffset = -1;

    // 当前 ConsumeQueue 最小消息物理偏移量
    @Getter
    @Setter
    private volatile long minLogicOffset = 0;

    private ConsumeQueueExt consumeQueueExt = null;

    public ConsumeQueue(final String topic, final int queueId, final String storePath/* users/zhuchenjian/store/consumequeue */, final int mappedFileSize /* 6000000 KB =  5.72 MB*/, final DefaultMessageStore defaultMessageStore) {
        this.mappedFileSize = mappedFileSize; /* 6000000 KB =  5.72 MB*/
        this.defaultMessageStore = defaultMessageStore;
        this.topic = topic;
        this.queueId = queueId;

        // users/zhuchenjian/store/consumequeue/topic/queueId
        String queueDir = storePath + File.separator + topic + File.separator + queueId;

        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);

        // 申请了一个 20 字节大小的缓冲区
        this.byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE /*20*/);

        if (defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt()) {
            // 默认是 false
            this.consumeQueueExt = new ConsumeQueueExt(
                    topic,
                    queueId,
                    StorePathConfigHelper.getStorePathConsumeQueueExt(defaultMessageStore.getMessageStoreConfig().getStorePathRootDir()), // /users/zhuchenjian/store/consumequeue_ext
                    defaultMessageStore.getMessageStoreConfig().getMappedFileSizeConsumeQueueExt(),
                    defaultMessageStore.getMessageStoreConfig().getBitMapLengthConsumeQueueExt()
            );
        }
    }

    /**
     * ConsumerQueue 启动阶段第一步：load
     */
    public boolean load() {
        // new MappedFileQueue(queueDir, mappedFileSize, null);
        boolean result = this.mappedFileQueue.load();
        log.info("load consume queue " + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        if (isExtReadEnable()) {
            result &= this.consumeQueueExt.load();
        }
        return result;
    }

    /**
     * ConsumerQueue 启动阶段第二步：recover
     */
    public void recover() {
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            int index = mappedFiles.size() - 3;
            if (index < 0) {
                // 文件夹下面不足3个文件，则从第一个文件开始
                index = 0;
            }

            // 文件大小
            int mappedFileSizeLogics = this.mappedFileSize;
            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();

            // 当前遍历中的文件的遍历偏移量，每个文件都是从 0 开始
            long mappedFileOffset = 0;
            long maxExtAddr = 1;
            while (true) {
                for (int i = 0; i < mappedFileSizeLogics; i += CQ_STORE_UNIT_SIZE /*因为CQData 每个是 20 字节*/) {

                    // 读取 前 20 个字节 start
                    long offset = byteBuffer.getLong();// 8个字节存储该条消息在 commitLog 的物理偏移量
                    int size = byteBuffer.getInt(); // 中间4个字节存储的一会消息的大小
                    long tagsCode = byteBuffer.getLong();// 后8个字节存储的时候该消息的 tag 的 hashcode 值
                    // 读取 前 20 个字节 end

                    if (offset >= 0 && size > 0) {
                        mappedFileOffset = i + CQ_STORE_UNIT_SIZE;
                        this.maxPhysicOffset = offset + size;
                        if (isExtAddr(tagsCode)) {
                            maxExtAddr = tagsCode;
                        }
                    } else {
                        log.info("recover current consume queue file over,  " + mappedFile.getFileName() + " " + offset + " " + size + " " + tagsCode);
                        break;
                    }
                }
                if (mappedFileOffset == mappedFileSizeLogics) {

                    // 上一个文件到文件尾了，则继续执行下一个文件
                    index++;
                    if (index >= mappedFiles.size()) {

                        // 所有文件都处理完，退出循环
                        log.info("recover last consume queue file over, last mapped file " + mappedFile.getFileName());
                        break;
                    } else {

                        // 还有下一个文件，继续
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();

                        // 当前遍历中的文件的遍历偏移量，每个文件都是从 0 开始
                        mappedFileOffset = 0;
                        log.info("recover next consume queue file, " + mappedFile.getFileName());
                    }
                } else {
                    log.info("recover current consume queue queue over " + mappedFile.getFileName() + " " + (processOffset + mappedFileOffset));
                    break;
                }
            }

            // 目录的位点
            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);
            if (isExtReadEnable()) {
                this.consumeQueueExt.recover();
                log.info("Truncate consume queue extend file by max {}", maxExtAddr);
                this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
            }
        }
    }

    /**
     * 从当前逻辑队列中根据时间搜索
     */
    public long getOffsetInQueueByTime(final long timestamp) {

        // 查询当前文件列表中，修改时间 >= 传入时间的第一个文件
        MappedFile mappedFile = this.mappedFileQueue.getMappedFileByTime(timestamp);
        if (mappedFile != null) {
            long offset;
            int low = minLogicOffset > mappedFile.getFileFromOffset() ? (int) (minLogicOffset - mappedFile.getFileFromOffset()) : 0;
            int high;
            int midOffset, targetOffset = -1, leftOffset = -1, rightOffset = -1;
            long leftIndexValue = -1L, rightIndexValue = -1L;
            long minPhysicOffset = this.defaultMessageStore.getMinPhyOffset();
            SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(0/*拿到整个文件的内容*/);
            if (null != sbr) {
                ByteBuffer byteBuffer = sbr.getByteBuffer();
                high = byteBuffer.limit() - CQ_STORE_UNIT_SIZE;
                try {
                    // 二分查找方法
                    while (high >= low) {
                        midOffset = (low + high) / (2 * CQ_STORE_UNIT_SIZE) * CQ_STORE_UNIT_SIZE;
                        byteBuffer.position(midOffset);
                        long phyOffset = byteBuffer.getLong();
                        int size = byteBuffer.getInt();
                        if (phyOffset < minPhysicOffset) {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            continue;
                        }

                        long storeTime = this.defaultMessageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                        if (storeTime < 0) {
                            return 0;
                        } else if (storeTime == timestamp) {
                            // 跳出循环的点
                            targetOffset = midOffset;
                            break;
                        } else if (storeTime > timestamp) {

                            // 向左移动20字节
                            high = midOffset - CQ_STORE_UNIT_SIZE;
                            rightOffset = midOffset;
                            rightIndexValue = storeTime;
                        } else {
                            // 向右移动20字节
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            leftIndexValue = storeTime;
                        }
                    }

                    if (targetOffset != -1) {

                        offset = targetOffset;
                    } else {
                        // 没有找到！！！
                        if (leftIndexValue == -1) {

                            offset = rightOffset;
                        } else if (rightIndexValue == -1) {

                            offset = leftOffset;
                        } else {
                            offset = Math.abs(timestamp - leftIndexValue) > Math.abs(timestamp - rightIndexValue) ? rightOffset : leftOffset;
                        }
                    }

                    return (mappedFile.getFileFromOffset() + offset) / CQ_STORE_UNIT_SIZE;// 计算得到逻辑偏移量
                } finally {
                    sbr.release();
                }
            }
        }
        return 0;
    }

    /**
     * 该方法的调用者： commitLog 恢复阶段调用
     *
     * 该方法主要是将 ConsumerQueue 有效数据文件  与 CommitLog 对齐，将超出部分的数据文件 删除。
     *
     * @param phyOffset 准确无误的最大消息物理偏移量
     */
    public void truncateDirtyLogicFiles(long phyOffset/*commitLog全局的最大物理偏移量*/) {
        int logicFileSize = this.mappedFileSize;
        this.maxPhysicOffset = phyOffset;
        long maxExtAddr = 1;
        while (true) {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            if (mappedFile != null) {
                ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
                mappedFile.setWrotePosition(0);
                mappedFile.setCommittedPosition(0);
                mappedFile.setFlushedPosition(0);
                for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE/*20*/) /*循环处理每个*/ {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();
                    if (0 == i /* consumeQueue 的第一个消息*/) {
                        if (offset/*第一个消息的物理偏移量*/ >= phyOffset/*commitLog全局的最大物理偏移量*/) {
                            // 说明 commitLog 部分文件被删除，或者 该 consumeQUEUe 中的数据是脏数据
                            this.mappedFileQueue.deleteLastMappedFile();
                            break /* 跳出 内层 for 循环 */;
                        } else {
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxPhysicOffset = offset + size;
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }
                        }
                    } else {
                        if (offset >= 0 && size > 0) {
                            if (offset >= phyOffset) {
                                return;
                            }
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxPhysicOffset = offset + size;
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }
                            if (pos == logicFileSize) {
                                return;
                            }
                        } else {
                            return;
                        }
                    }
                }
            } else {
                break;
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
        }
    }

    public long getLastOffset() {
        long lastOffset = -1;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile != null) {
            int position = mappedFile.getWrotePosition() - CQ_STORE_UNIT_SIZE;
            if (position < 0) {
                position = 0;
            }

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            byteBuffer.position(position);
            for (int i = 0; i < this.mappedFileSize; i += CQ_STORE_UNIT_SIZE) {
                long offset = byteBuffer.getLong();
                int size = byteBuffer.getInt();
                byteBuffer.getLong();

                if (offset >= 0 && size > 0) {
                    lastOffset = offset + size;
                } else {
                    break;
                }
            }
        }

        return lastOffset;
    }

    public boolean flush(final int flushLeastPages) {
        boolean result = this.mappedFileQueue.flush(flushLeastPages);
        if (isExtReadEnable()) {
            result = result & this.consumeQueueExt.flush(flushLeastPages);
        }

        return result;
    }

    public int deleteExpiredFile(long offset /*删除 minOffset 之前的数据*/) {
        int cnt = this.mappedFileQueue.deleteExpiredFileByOffset(offset, CQ_STORE_UNIT_SIZE);
        this.correctMinOffset(offset);
        return cnt;
    }

    public void correctMinOffset(long phyMinOffset) {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        long minExtAddr = 1;
        if (mappedFile != null) {
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(0);
            if (result != null) {
                try {
                    for (int i = 0; i < result.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                        long offsetPy = result.getByteBuffer().getLong();
                        result.getByteBuffer().getInt();
                        long tagsCode = result.getByteBuffer().getLong();

                        if (offsetPy >= phyMinOffset) {
                            this.minLogicOffset = mappedFile.getFileFromOffset() + i;
                            log.info("Compute logical min offset: {}, topic: {}, queueId: {}", this.getMinOffsetInQueue(), this.topic, this.queueId);
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                minExtAddr = tagsCode;
                            }
                            break;
                        }
                    }
                } catch (Exception e) {
                    log.error("Exception thrown when correctMinOffset", e);
                } finally {
                    result.release();
                }
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMinAddress(minExtAddr);
        }
    }

    public long getMinOffsetInQueue() {
        return this.minLogicOffset / CQ_STORE_UNIT_SIZE;
    }

    /**
     * 上层 DefaultMessageStore 内部的 异步线程 调用，存储主模块开启了一个 异步构建的  ConsumerQueue 文件和索引文件的线程
     * 该线程启动后，会持续关注 commitLog 文件，当 commitLog 文件内有新数据写入后，立马读出来 将其封装成 {@link DispatchRequest} 对象，
     * 转发给 ConsumerQueue 或者 IndexService
     *
     * @param request 由msg封装而来，只是没有body
     */
    public void putMessagePositionInfoWrapper(DispatchRequest request) {
        final int maxRetries = 30;

        // 当前是否可写
        // return (this.flagBits & (NOT_WRITEABLE_BIT | WRITE_LOGICS_QUEUE_ERROR_BIT | WRITE_INDEX_FILE_ERROR_BIT)) == 0;
        boolean canWrite = this.defaultMessageStore.getRunningFlags().isCQWriteable();

        // 最多30次
        for (int i = 0; i < maxRetries && canWrite/*当前存储模块是否可写*/; i++) {

            // 消息 tag 的 hash
            long tagsCode = request.getTagsCode();
            if (isExtWriteEnable()) {
                // 一般不进来
                ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                cqExtUnit.setFilterBitMap(request.getBitMap());
                cqExtUnit.setMsgStoreTime(request.getStoreTimestamp());
                cqExtUnit.setTagsCode(request.getTagsCode());

                long extAddr = this.consumeQueueExt.put(cqExtUnit);
                if (isExtAddr(extAddr)) {
                    tagsCode = extAddr;
                } else {
                    log.warn("Save consume queue extend fail, So just save tagsCode! {}, topic:{}, queueId:{}, offset:{}", cqExtUnit, topic, queueId, request.getCommitLogOffset());
                }
            }

            // 正常情况返回 true
            long commitLogOffset = request.getCommitLogOffset();
            // 消息大小
            int msgSize = request.getMsgSize();

            // 在消费队列中的偏移量
            long consumeQueueOffset = request.getConsumeQueueOffset();
            boolean result = this.putMessagePositionInfo(commitLogOffset, msgSize, tagsCode, consumeQueueOffset);

            StoreCheckpoint storeCheckpoint = this.defaultMessageStore.getStoreCheckpoint();
            if (result) {
                // 设置 checkPoint

                if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE || this.defaultMessageStore.getMessageStoreConfig().isEnableDLegerCommitLog()) {

                    // 更新 checkPoint 的时间
                    storeCheckpoint.setPhysicMsgTimestamp(request.getStoreTimestamp());
                }

                // checkPoint 记录最后一条CQData 所管理 msg 的存储时间
                storeCheckpoint.setLogicsMsgTimestamp(request.getStoreTimestamp());
                return;
            } else {
                // XXX: warn and notify me
                log.warn("[BUG]put commit log position info to " + topic + ":" + queueId + " " + request.getCommitLogOffset() + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }

        // XXX: warn and notify me
        log.error("[BUG]consume queue can not write, {} {}", this.topic, this.queueId);
        this.defaultMessageStore.getRunningFlags().makeLogicsQueueError();
    }

    /**
     * @param offset 当前msg的物理 offset
     * @param size 当前msg大小
     * @param tagsCode 当前msg的tagCode
     * @param cqOffset 当前msg 的 逻辑偏移量 (ConsumerQueue 内的偏移量，转化成 真是物理偏移量的算法：消息的逻辑偏移量 * 20)
     */
    private boolean putMessagePositionInfo(final long offset, final int size, final long tagsCode, final long cqOffset/*消息的逻辑偏移量*/) {

        if (offset + size <= this.maxPhysicOffset) {

            // 重复了？
            log.warn("Maybe try to build consume queue repeatedly maxPhysicOffset={} phyOffset={}", maxPhysicOffset, offset);
            return true;
        }

        this.byteBufferIndex.flip();
        this.byteBufferIndex.limit(CQ_STORE_UNIT_SIZE/*20*/);

        // 20 个字节加入到临时缓冲区
        this.byteBufferIndex.putLong(offset); // commitLog 偏移量
        this.byteBufferIndex.putInt(size);
        this.byteBufferIndex.putLong(tagsCode);

        // 计数出在逻辑队列（consumeQueue文件）中的真是物理偏移量
        final long expectLogicOffset = cqOffset * CQ_STORE_UNIT_SIZE;

        // 根据真是物理偏移量获取对应的文件
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(expectLogicOffset);

        if (mappedFile != null) {

            if (mappedFile.isFirstCreateInQueue() && cqOffset != 0 && mappedFile.getWrotePosition() == 0) {
                this.minLogicOffset = expectLogicOffset;
                this.mappedFileQueue.setFlushedWhere(expectLogicOffset);
                this.mappedFileQueue.setCommittedWhere(expectLogicOffset);

                // 补白？？？ TODO
                this.fillPreBlank(mappedFile, expectLogicOffset);
                log.info("fill pre blank space " + mappedFile.getFileName() + " " + expectLogicOffset + " " + mappedFile.getWrotePosition());
            }

            if (cqOffset != 0) {

                // 当前 cq 文件写入点的物理偏移量
                long currentLogicOffset = mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();

                if (expectLogicOffset < currentLogicOffset) {
                    // 复写操作
                    String msg = "Build  consume queue repeatedly, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}";
                    log.warn(msg, expectLogicOffset, currentLogicOffset, this.topic, this.queueId, expectLogicOffset - currentLogicOffset);
                    return true;
                }

                if (expectLogicOffset != currentLogicOffset) {
                    // TODO ?????
                    // 错误的插入
                    String msg = "[BUG]logic queue order maybe wrong, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}";
                    LOG_ERROR.warn(msg, expectLogicOffset, currentLogicOffset, this.topic, this.queueId, expectLogicOffset - currentLogicOffset);
                }
            }

            this.maxPhysicOffset = offset + size;
            // 追加数据到 consumeQueue mf 文件
            return mappedFile.appendMessage(this.byteBufferIndex.array());
        }
        return false;
    }

    private void fillPreBlank(final MappedFile mappedFile, final long untilWhere) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        byteBuffer.putLong(0L);
        byteBuffer.putInt(Integer.MAX_VALUE);
        byteBuffer.putLong(0L);

        int until = (int) (untilWhere % this.mappedFileQueue.getMappedFileSize());
        for (int i = 0; i < until; i += CQ_STORE_UNIT_SIZE) {
            mappedFile.appendMessage(byteBuffer.array());
        }
    }

    /**
     * bufferConsumeQueue 数据范围：
     * 1.如果 offset 命中的文件不是正在顺序写的文件的话，则范围是[offset表示的这条消息 , 文件尾]
     * 2.如果 offset 命中的文件是正在顺序写的文件的话，则范围是[offset表示的这条消息 , 文件名 + wrotePosition]
     */
    public SelectMappedBufferResult getIndexBuffer(final long startIndex/*逻辑offset，从1开始，每条存储一条消息offset + 1*/) {
        int mappedFileSize = this.mappedFileSize;
        long offset/*根据逻辑offset 得到物理偏移量*/ = startIndex * CQ_STORE_UNIT_SIZE;
        if (offset >= this.getMinLogicOffset() /*正常的*/) {
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset/*根据物理偏移量查询包含该偏移量的mappedFile文件*/);
            if (mappedFile != null) {
                long pos = offset/*consumeQueue文件中的物理偏移量*/ % mappedFileSize/*consumeQueue中每个文件的大小600W字节*/;
                return mappedFile.selectMappedBuffer((int) (pos));
            }
        }
        return null;
    }

    public ConsumeQueueExt.CqExtUnit getExt(final long offset) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset);
        }
        return null;
    }

    public boolean getExt(final long offset, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset, cqExtUnit);
        }
        return false;
    }

    public long rollNextFile(final long index) {
        int mappedFileSize = this.mappedFileSize;
        int totalUnitsInFile = mappedFileSize / CQ_STORE_UNIT_SIZE;
        return index + totalUnitsInFile - index % totalUnitsInFile;
    }

    public void destroy() {
        this.maxPhysicOffset = -1;
        this.minLogicOffset = 0;
        this.mappedFileQueue.destroy();
        if (isExtReadEnable()) {
            this.consumeQueueExt.destroy();
        }
    }

    public long getMessageTotalInQueue() {
        return this.getMaxOffsetInQueue() - this.getMinOffsetInQueue();
    }

    public long getMaxOffsetInQueue() {
        return this.mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE;
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
        if (isExtReadEnable()) {
            this.consumeQueueExt.checkSelf();
        }
    }

    protected boolean isExtReadEnable() {
        return this.consumeQueueExt != null;
    }

    protected boolean isExtWriteEnable() {
        return this.consumeQueueExt != null && this.defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt();
    }

    /**
     * Check {@code tagsCode} is address of extend file or tags code.
     */
    public boolean isExtAddr(long tagsCode) {
        return ConsumeQueueExt.isExtAddr(tagsCode);
    }
}
