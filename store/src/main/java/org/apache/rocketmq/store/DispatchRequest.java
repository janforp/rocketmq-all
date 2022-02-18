package org.apache.rocketmq.store;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public class DispatchRequest {

    private final String topic;

    private final int queueId;

    private final long commitLogOffset;

    /**
     * 消息大小
     */
    private int msgSize;

    private final long tagsCode;

    private final long storeTimestamp;

    private final long consumeQueueOffset;

    private final String keys;

    /**
     * 成功失败
     */
    private final boolean success;

    private final String uniqKey;

    private final int sysFlag;

    private final long preparedTransactionOffset;

    private final Map<String, String> propertiesMap;

    private byte[] bitMap;

    private int bufferSize = -1;//the buffer size maybe larger than the msg size if the message is wrapped by something

    public DispatchRequest(
            final String topic,
            final int queueId,
            final long commitLogOffset,
            final int msgSize,
            final long tagsCode,
            final long storeTimestamp,
            final long consumeQueueOffset,
            final String keys,
            final String uniqKey,
            final int sysFlag,
            final long preparedTransactionOffset,
            final Map<String, String> propertiesMap
    ) {
        this.topic = topic;
        this.queueId = queueId;
        this.commitLogOffset = commitLogOffset;
        this.msgSize = msgSize;
        this.tagsCode = tagsCode;
        this.storeTimestamp = storeTimestamp;
        this.consumeQueueOffset = consumeQueueOffset;
        this.keys = keys;
        this.uniqKey = uniqKey;

        this.sysFlag = sysFlag;
        this.preparedTransactionOffset = preparedTransactionOffset;
        this.success = true;
        this.propertiesMap = propertiesMap;
    }

    public DispatchRequest(int size) {
        this.topic = "";
        this.queueId = 0;
        this.commitLogOffset = 0;
        this.msgSize = size;
        this.tagsCode = 0;
        this.storeTimestamp = 0;
        this.consumeQueueOffset = 0;
        this.keys = "";
        this.uniqKey = null;
        this.sysFlag = 0;
        this.preparedTransactionOffset = 0;
        this.success = false;
        this.propertiesMap = null;
    }

    public DispatchRequest(int size, boolean success) {
        this.topic = "";
        this.queueId = 0;
        this.commitLogOffset = 0;
        this.msgSize = size;
        this.tagsCode = 0;
        this.storeTimestamp = 0;
        this.consumeQueueOffset = 0;
        this.keys = "";
        this.uniqKey = null;
        this.sysFlag = 0;
        this.preparedTransactionOffset = 0;
        this.success = success;
        this.propertiesMap = null;
    }

}
