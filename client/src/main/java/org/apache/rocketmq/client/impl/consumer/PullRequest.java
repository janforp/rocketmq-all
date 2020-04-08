package org.apache.rocketmq.client.impl.consumer;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.message.MessageQueue;

public class PullRequest {

    @Setter
    @Getter
    private String consumerGroup;

    @Setter
    @Getter
    private MessageQueue messageQueue;

    @Setter
    @Getter
    private ProcessQueue processQueue;

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
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        PullRequest other = (PullRequest) obj;
        if (consumerGroup == null) {
            if (other.consumerGroup != null) {
                return false;
            }
        } else if (!consumerGroup.equals(other.consumerGroup)) {
            return false;
        }
        if (messageQueue == null) {
            if (other.messageQueue != null) {
                return false;
            }
        } else if (!messageQueue.equals(other.messageQueue)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "PullRequest [consumerGroup=" + consumerGroup + ", messageQueue=" + messageQueue
                + ", nextOffset=" + nextOffset + "]";
    }
}
