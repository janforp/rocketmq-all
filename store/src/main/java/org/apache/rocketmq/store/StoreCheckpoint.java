package org.apache.rocketmq.store;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.index.IndexService;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

/**
 * 安全点
 */
public class StoreCheckpoint {

    /**
     * /Users/zhuchenjian/Documents/code/learn/rocketmq/rocketmq-all/conf/home/broker/store/checkpoint 文件
     */
    private final FileChannel fileChannel;

    /**
     * /Users/zhuchenjian/Documents/code/learn/rocketmq/rocketmq-all/conf/home/broker/store/checkpoint 文件
     */
    private final MappedByteBuffer mappedByteBuffer;

    /**
     * commitLog 同步/异步刷盘成功的时间
     *
     * @see CommitLog.FlushRealTimeService#run() 异步刷盘
     * @see ConsumeQueue#putMessagePositionInfoWrapper(org.apache.rocketmq.store.DispatchRequest)
     */
    @Getter
    @Setter
    private volatile long physicMsgTimestamp = 0;

    /**
     * ConsumeQueue 刷盘成功或者条添加索引成功的时间
     *
     * @see DefaultMessageStore.FlushConsumeQueueService
     * @see ConsumeQueue#putMessagePositionInfoWrapper(org.apache.rocketmq.store.DispatchRequest)
     */
    @Getter
    @Setter
    private volatile long logicsMsgTimestamp = 0;

    /**
     * indexFile 刷盘成功的时间
     *
     * @see IndexService#flush(org.apache.rocketmq.store.index.IndexFile)
     */
    @Getter
    @Setter
    private volatile long indexMsgTimestamp = 0;

    public StoreCheckpoint(final String scpPath/*/Users/zhuchenjian/Documents/code/learn/rocketmq/rocketmq-all/conf/home/broker/store/checkpoint文件*/) throws IOException {
        File file = new File(scpPath);
        MappedFile.ensureDirOK(file.getParent());
        boolean fileExists = file.exists();

        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        this.fileChannel = randomAccessFile.getChannel();
        this.mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, MappedFile.OS_PAGE_SIZE);

        if (fileExists) {
            this.physicMsgTimestamp = this.mappedByteBuffer.getLong(0);
            this.logicsMsgTimestamp = this.mappedByteBuffer.getLong(8);
            this.indexMsgTimestamp = this.mappedByteBuffer.getLong(16);
        }
    }

    public void shutdown() {
        this.flush();

        // unmap mappedByteBuffer
        MappedFileAssist.clean(this.mappedByteBuffer);

        try {
            this.fileChannel.close();
        } catch (IOException e) {
            // log.error("Failed to properly close the channel", e);
        }
    }

    public void flush() {
        this.mappedByteBuffer.putLong(0, this.physicMsgTimestamp);
        this.mappedByteBuffer.putLong(8, this.logicsMsgTimestamp);
        this.mappedByteBuffer.putLong(16, this.indexMsgTimestamp);

        // 写到文件中去
        this.mappedByteBuffer.force();
    }

    /**
     * 取 commitLog 文件跟 consumeQueue 文件 跟 indexFile 文件的最后存储时间的最小值
     *
     * 有些公司是不会开启 indexFile 的，可以通过配置修改
     *
     * @see MessageStoreConfig#messageIndexEnable 打开才有 索引
     * @see MessageStoreConfig#messageIndexSafe 打开才有索引
     */
    public long getMinTimestampIndex() {
        long minTimestamp = this.getMinTimestamp();
        return Math.min(minTimestamp, this.indexMsgTimestamp);
    }

    /**
     * 取 commitLog 文件跟 consumeQueue 文件的最后存储时间的最小值
     */
    public long getMinTimestamp() {
        long min = Math.min(this.physicMsgTimestamp, this.logicsMsgTimestamp);

        // TODO ？ 为何减去3秒？
        min = min - 1000 * 3;
        if (min < 0) {
            min = 0;
        }

        return min;
    }
}
