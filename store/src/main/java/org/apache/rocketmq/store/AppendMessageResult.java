package org.apache.rocketmq.store;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * When write a message to the commit log, returns results
 */
@ToString
public class AppendMessageResult {

    // Return code
    @Getter
    @Setter
    private AppendMessageStatus status;

    // Where to start writing
    @Getter
    @Setter
    private long wroteOffset;

    /**
     * Write Bytes
     * 本次总共追加（写入）的总字节数量
     */
    @Getter
    @Setter
    private int wroteBytes;

    // Message ID
    @Getter
    @Setter
    private String msgId;

    // Message storage timestamp

    /**
     * 消息的 storeTimestamp
     */
    @Getter
    @Setter
    private long storeTimestamp;

    // Consume queue's offset(step by one)
    @Getter
    @Setter
    private long logicsOffset;

    @Getter
    @Setter
    private long pagecacheRT;

    @Getter
    @Setter
    private int msgNum = 1;

    public AppendMessageResult(AppendMessageStatus status) {
        this(status, 0, 0, "", 0, 0, 0);
    }

    //  new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen, msgId, msgInner.getStoreTimestamp(), queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
    public AppendMessageResult(AppendMessageStatus status, long wroteOffset, int wroteBytes, String msgId, long storeTimestamp, long logicsOffset, long pagecacheRT) {
        this.status = status;
        this.wroteOffset = wroteOffset;
        this.wroteBytes = wroteBytes;
        this.msgId = msgId;
        this.storeTimestamp = storeTimestamp;
        this.logicsOffset = logicsOffset;
        this.pagecacheRT = pagecacheRT;
    }

    public boolean isOk() {
        return this.status == AppendMessageStatus.PUT_OK;
    }
}