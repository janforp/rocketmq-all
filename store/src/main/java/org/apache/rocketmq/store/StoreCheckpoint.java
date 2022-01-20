package org.apache.rocketmq.store;

import lombok.Getter;
import lombok.Setter;

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

    private final FileChannel fileChannel;

    private final MappedByteBuffer mappedByteBuffer;

    /**
     * 最后一次刷盘成功的时间
     *
     * @see CommitLog.FlushRealTimeService#run() 异步刷盘
     * @see ConsumeQueue#putMessagePositionInfoWrapper(org.apache.rocketmq.store.DispatchRequest)
     */
    @Getter
    @Setter
    private volatile long physicMsgTimestamp = 0;

    /**
     * @see DefaultMessageStore.FlushConsumeQueueService
     * @see ConsumeQueue#putMessagePositionInfoWrapper(org.apache.rocketmq.store.DispatchRequest)
     */
    @Getter
    @Setter
    private volatile long logicsMsgTimestamp = 0;

    @Getter
    @Setter
    private volatile long indexMsgTimestamp = 0;

    public StoreCheckpoint(final String scpPath) throws IOException {
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
        MappedFile.clean(this.mappedByteBuffer);

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

    public long getMinTimestampIndex() {
        return Math.min(this.getMinTimestamp(), this.indexMsgTimestamp);
    }

    public long getMinTimestamp() {
        long min = Math.min(this.physicMsgTimestamp, this.logicsMsgTimestamp);

        min -= 1000 * 3;
        if (min < 0) {
            min = 0;
        }

        return min;
    }
}
