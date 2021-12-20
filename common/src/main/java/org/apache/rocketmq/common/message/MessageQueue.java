package org.apache.rocketmq.common.message;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@NoArgsConstructor
@AllArgsConstructor
@ToString
public class MessageQueue implements Comparable<MessageQueue>, Serializable {

    private static final long serialVersionUID = 6191200464116433425L;

    @Getter
    @Setter
    private String topic;

    @Getter
    @Setter
    private String brokerName;

    @Getter
    @Setter
    private int queueId;

    @Override
    public int compareTo(MessageQueue messageQueue) {
        {
            int result = this.topic.compareTo(messageQueue.topic);
            if (result != 0) {
                return result;
            }
        }

        {
            int result = this.brokerName.compareTo(messageQueue.brokerName);
            if (result != 0) {
                return result;
            }
        }

        return this.queueId - messageQueue.queueId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
        result = prime * result + queueId;
        result = prime * result + ((topic == null) ? 0 : topic.hashCode());
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
        MessageQueue other = (MessageQueue) obj;
        if (brokerName == null) {
            if (other.brokerName != null) {
                return false;
            }
        } else if (!brokerName.equals(other.brokerName)) {
            return false;
        }
        if (queueId != other.queueId) {
            return false;
        }
        if (topic == null) {
            if (other.topic != null) {
                return false;
            }
        } else if (!topic.equals(other.topic)) {
            return false;
        }
        return true;
    }
}
