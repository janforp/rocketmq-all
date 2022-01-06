package org.apache.rocketmq.client.consumer.listener;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Consumer Orderly consumption context
 */
public class ConsumeOrderlyContext {

    @Getter
    private final MessageQueue messageQueue;

    @Getter
    @Setter
    private boolean autoCommit = true;

    @Getter
    @Setter
    private long suspendCurrentQueueTimeMillis = -1;

    public ConsumeOrderlyContext(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }
}