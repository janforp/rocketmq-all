package org.apache.rocketmq.store.config;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.annotation.ImportantField;
import org.apache.rocketmq.store.ConsumeQueue;

import java.io.File;

public class MessageStoreConfig {

    //The root directory in which the log data is kept
    // 如：/Users/zhuchenjian
    @ImportantField
    @Getter
    @Setter
    private String storePathRootDir = System.getProperty("user.home") + File.separator + "store";

    //The directory in which the commitlog is kept
    @ImportantField
    @Getter
    @Setter
    private String storePathCommitLog = System.getProperty("user.home") + File.separator + "store" + File.separator + "commitlog";

    // CommitLog file size,default is 1G
    @Getter
    @Setter
    private int mappedFileSizeCommitLog = 1024 * 1024 * 1024;

    // ConsumeQueue file size,default is 30W
    @Setter
    private int mappedFileSizeConsumeQueue = 300000 * ConsumeQueue.CQ_STORE_UNIT_SIZE;

    // enable consume queue ext
    @Getter
    @Setter
    private boolean enableConsumeQueueExt = false;

    // ConsumeQueue extend file size, 48M
    @Getter
    @Setter
    private int mappedFileSizeConsumeQueueExt = 48 * 1024 * 1024;

    // Bit count of filter bit map.
    // this will be set by pipe of calculate filter bit map.
    @Getter
    @Setter
    private int bitMapLengthConsumeQueueExt = 64;

    // CommitLog flush interval
    // flush data to disk
    @ImportantField
    @Getter
    @Setter
    private int flushIntervalCommitLog = 500;

    // Only used if TransientStorePool enabled
    // flush data to FileChannel
    @ImportantField
    @Getter
    @Setter
    private int commitIntervalCommitLog = 200;

    /**
     * introduced since 4.0.x. Determine whether to use mutex reentrantLock when putting message.<br/>
     * By default it is set to false indicating using spin lock when putting message.
     */
    @Getter
    @Setter
    private boolean useReentrantLockWhenPutMessage = false;

    // Whether schedule flush,default is real-time
    @ImportantField
    @Getter
    @Setter
    private boolean flushCommitLogTimed = false;

    // ConsumeQueue flush interval
    @Getter
    @Setter
    private int flushIntervalConsumeQueue = 1000;

    // Resource reclaim interval
    @Getter
    @Setter
    private int cleanResourceInterval = 10000;

    // CommitLog removal interval
    @Getter
    @Setter
    private int deleteCommitLogFilesInterval = 100;

    // ConsumeQueue removal interval
    @Getter
    @Setter
    private int deleteConsumeQueueFilesInterval = 100;

    @Getter
    @Setter
    private int destroyMapedFileIntervalForcibly = 1000 * 120;

    @Getter
    @Setter
    private int redeleteHangedFileInterval = 1000 * 120;

    // When to delete,default is at 4 am
    @ImportantField
    @Getter
    @Setter
    private String deleteWhen = "04";

    @Setter
    private int diskMaxUsedSpaceRatio = 75;

    // The number of hours to keep a log file before deleting it (in hours)
    @ImportantField
    @Getter
    @Setter
    private int fileReservedTime = 72;

    // Flow control for ConsumeQueue
    @Getter
    @Setter
    private int putMsgIndexHightWater = 600000;

    // The maximum size of message,default is 4M
    @Getter
    @Setter
    private int maxMessageSize = 1024 * 1024 * 4;

    // Whether check the CRC32 of the records consumed.
    // This ensures no on-the-wire or on-disk corruption to the messages occurred.
    // This check adds some overhead,so it may be disabled in cases seeking extreme performance.
    @Getter
    @Setter
    private boolean checkCRCOnRecover = true;

    // How many pages are to be flushed when flush CommitLog
    @Getter
    @Setter
    private int flushCommitLogLeastPages = 4;

    // How many pages are to be committed when commit data to file
    @Getter
    @Setter
    private int commitCommitLogLeastPages = 4;

    // Flush page size when the disk in warming state
    @Getter
    @Setter
    private int flushLeastPagesWhenWarmMapedFile = 1024 / 4 * 16;

    // How many pages are to be flushed when flush ConsumeQueue
    @Getter
    @Setter
    private int flushConsumeQueueLeastPages = 2;

    @Getter
    @Setter
    private int flushCommitLogThoroughInterval = 1000 * 10;

    @Getter
    @Setter
    private int commitCommitLogThoroughInterval = 200;

    @Getter
    @Setter
    private int flushConsumeQueueThoroughInterval = 1000 * 60;

    @ImportantField
    @Getter
    @Setter
    private int maxTransferBytesOnMessageInMemory = 1024 * 256;

    @ImportantField
    @Getter
    @Setter
    private int maxTransferCountOnMessageInMemory = 32;

    @ImportantField
    @Getter
    @Setter
    private int maxTransferBytesOnMessageInDisk = 1024 * 64;

    @ImportantField
    @Getter
    @Setter
    private int maxTransferCountOnMessageInDisk = 8;

    @ImportantField
    @Getter
    @Setter
    private int accessMessageInMemoryMaxRatio = 40;

    @ImportantField
    @Getter
    @Setter
    private boolean messageIndexEnable = true;

    @Getter
    @Setter
    private int maxHashSlotNum = 5000000;

    @Getter
    @Setter
    private int maxIndexNum = 5000000 * 4;

    @Getter
    @Setter
    private int maxMsgsNumBatch = 64;

    @ImportantField
    @Getter
    @Setter
    private boolean messageIndexSafe = false;

    @Getter
    @Setter
    private int haListenPort = 10912;

    @Getter
    @Setter
    private int haSendHeartbeatInterval = 1000 * 5;

    @Getter
    @Setter
    private int haHousekeepingInterval = 1000 * 20;

    @Getter
    @Setter
    private int haTransferBatchSize = 1024 * 32;

    @ImportantField
    @Getter
    @Setter
    private String haMasterAddress = null;

    @Getter
    @Setter
    private int haSlaveFallbehindMax = 1024 * 1024 * 256;

    @ImportantField
    @Getter
    private BrokerRole brokerRole = BrokerRole.ASYNC_MASTER;

    @ImportantField
    @Getter
    private FlushDiskType flushDiskType = FlushDiskType.ASYNC_FLUSH;

    @Getter
    @Setter
    private int syncFlushTimeout = 1000 * 5;

    @Getter
    @Setter
    private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";

    @Getter
    @Setter
    private long flushDelayOffsetInterval = 1000 * 10;

    @ImportantField
    @Getter
    @Setter
    private boolean cleanFileForciblyEnable = true;

    @Getter
    @Setter
    private boolean warmMapedFileEnable = false;

    @Getter
    @Setter
    private boolean offsetCheckInSlave = false;

    @Getter
    @Setter
    private boolean debugLockEnable = false;

    @Getter
    @Setter
    private boolean duplicationEnable = false;

    @Getter
    @Setter
    private boolean diskFallRecorded = true;

    @Getter
    @Setter
    private long osPageCacheBusyTimeOutMills = 1000;

    @Getter
    @Setter
    private int defaultQueryMaxNum = 32;

    @ImportantField
    @Setter
    private boolean transientStorePoolEnable = false;

    @Getter
    @Setter
    private int transientStorePoolSize = 5;

    @Getter
    @Setter
    private boolean fastFailIfNoBufferInStorePool = false;

    @Getter
    @Setter
    private boolean enableDLegerCommitLog = false;

    private String dLegerGroup;

    private String dLegerPeers;

    private String dLegerSelfId;

    public int getMappedFileSizeConsumeQueue() {
        int factor = (int) Math.ceil(this.mappedFileSizeConsumeQueue / (ConsumeQueue.CQ_STORE_UNIT_SIZE * 1.0));
        return factor * ConsumeQueue.CQ_STORE_UNIT_SIZE;
    }

    public int getDiskMaxUsedSpaceRatio() {
        if (this.diskMaxUsedSpaceRatio < 10) {
            return 10;
        }

        if (this.diskMaxUsedSpaceRatio > 95) {
            return 95;
        }

        return diskMaxUsedSpaceRatio;
    }

    public void setBrokerRole(String brokerRole) {
        this.brokerRole = BrokerRole.valueOf(brokerRole);
    }

    public void setBrokerRole(BrokerRole brokerRole) {
        this.brokerRole = brokerRole;
    }

    public void setFlushDiskType(String type) {
        this.flushDiskType = FlushDiskType.valueOf(type);
    }

    public void setFlushDiskType(FlushDiskType flushDiskType) {
        this.flushDiskType = flushDiskType;
    }

    /**
     * Enable transient commitLog store pool only if transientStorePoolEnable is true and the FlushDiskType is
     * ASYNC_FLUSH
     *
     * @return <tt>true</tt> or <tt>false</tt>
     */
    public boolean isTransientStorePoolEnable() {
        return transientStorePoolEnable && FlushDiskType.ASYNC_FLUSH == getFlushDiskType() && BrokerRole.SLAVE != getBrokerRole();
    }

    public String getdLegerSelfId() {
        return dLegerSelfId;
    }

    public String getdLegerGroup() {
        return dLegerGroup;
    }

    public String getdLegerPeers() {
        return dLegerPeers;
    }

    public void setdLegerGroup(String dLegerGroup) {
        this.dLegerGroup = dLegerGroup;
    }

    public void setdLegerPeers(String dLegerPeers) {
        this.dLegerPeers = dLegerPeers;
    }

    public void setdLegerSelfId(String dLegerSelfId) {
        this.dLegerSelfId = dLegerSelfId;
    }
}