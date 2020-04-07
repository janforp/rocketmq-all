package org.apache.rocketmq.common.protocol.body;

import lombok.Data;
import org.apache.rocketmq.common.UtilAll;

@Data
public class ProcessQueueInfo {

    private long commitOffset;

    private long cachedMsgMinOffset;

    private long cachedMsgMaxOffset;

    private int cachedMsgCount;

    private int cachedMsgSizeInMiB;

    private long transactionMsgMinOffset;

    private long transactionMsgMaxOffset;

    private int transactionMsgCount;

    private boolean locked;

    private long tryUnlockTimes;

    private long lastLockTimestamp;

    private boolean droped;

    private long lastPullTimestamp;

    private long lastConsumeTimestamp;

    @Override
    public String toString() {
        return "ProcessQueueInfo [commitOffset=" + commitOffset + ", cachedMsgMinOffset="
                + cachedMsgMinOffset + ", cachedMsgMaxOffset=" + cachedMsgMaxOffset
                + ", cachedMsgCount=" + cachedMsgCount + ", cachedMsgSizeInMiB=" + cachedMsgSizeInMiB
                + ", transactionMsgMinOffset=" + transactionMsgMinOffset
                + ", transactionMsgMaxOffset=" + transactionMsgMaxOffset + ", transactionMsgCount="
                + transactionMsgCount + ", locked=" + locked + ", tryUnlockTimes=" + tryUnlockTimes
                + ", lastLockTimestamp=" + UtilAll.timeMillisToHumanString(lastLockTimestamp) + ", droped="
                + droped + ", lastPullTimestamp=" + UtilAll.timeMillisToHumanString(lastPullTimestamp)
                + ", lastConsumeTimestamp=" + UtilAll.timeMillisToHumanString(lastConsumeTimestamp) + "]";
    }
}
