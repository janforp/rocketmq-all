package org.apache.rocketmq.common.protocol.route;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 主题队列分布 map
 */
@ToString
public class QueueData implements Comparable<QueueData> {

    /**
     * 该队列所在的 broker 名称
     */
    @Setter
    @Getter
    private String brokerName;

    /**
     * 读队列数
     */
    @Setter
    @Getter
    private int readQueueNums;

    /**
     * 写队列数
     */
    @Setter
    @Getter
    private int writeQueueNums;

    /**
     * 权限
     */
    @Setter
    @Getter
    private int perm;

    @Setter
    @Getter
    private int topicSynFlag;

    @Override
    public int compareTo(QueueData o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
        result = prime * result + perm;
        result = prime * result + readQueueNums;
        result = prime * result + writeQueueNums;
        result = prime * result + topicSynFlag;
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
        QueueData other = (QueueData) obj;
        if (brokerName == null) {
            if (other.brokerName != null) {
                return false;
            }
        } else if (!brokerName.equals(other.brokerName)) {
            return false;
        }
        if (perm != other.perm) {
            return false;
        }
        if (readQueueNums != other.readQueueNums) {
            return false;
        }
        if (writeQueueNums != other.writeQueueNums) {
            return false;
        }
        return topicSynFlag == other.topicSynFlag;
    }
}
