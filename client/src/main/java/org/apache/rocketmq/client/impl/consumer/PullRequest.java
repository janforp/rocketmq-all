package org.apache.rocketmq.client.impl.consumer;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 封装拉拉消息请求的具体数据
 */
@ToString
public class PullRequest {

    // 消费者组
    @Setter
    @Getter
    private String consumerGroup;

    // 队列元数据
    @Setter
    @Getter
    private MessageQueue messageQueue;

    // 本地快照
    @Setter
    @Getter
    private ProcessQueue processQueue;

    // 本次 拉消息请求的时候使用的 offset 值，很重要，服务器端需要根据该字段定位消息，然后才能获取一批消息
    @Setter
    @Getter
    private long nextOffset;

    @Setter
    @Getter
    private boolean lockedFirst = false;

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((consumerGroup == null) ? 0 : consumerGroup.hashCode());
        result = prime * result + ((messageQueue == null) ? 0 : messageQueue.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that == null) {
            return false;
        }
        if (getClass() != that.getClass()) {
            return false;
        }
        PullRequest thatPq = (PullRequest) that;
        if (consumerGroup == null) {
            if (thatPq.consumerGroup != null) {
                return false;
            }
        } else if (!consumerGroup.equals(thatPq.consumerGroup)) {
            return false;
        }
        if (messageQueue == null) {
            return thatPq.messageQueue == null;
        } else {
            return messageQueue.equals(thatPq.messageQueue);
        }
    }
}
