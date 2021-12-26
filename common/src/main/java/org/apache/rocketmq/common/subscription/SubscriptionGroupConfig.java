package org.apache.rocketmq.common.subscription;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.rocketmq.common.MixAll;

@Getter
@Setter
@ToString
public class SubscriptionGroupConfig {

    private String groupName;

    /**
     * 该消费者是否被打开
     *
     * @see org.apache.rocketmq.broker.subscription.SubscriptionGroupManager#disableConsume(java.lang.String)
     */
    private boolean consumeEnable = true;

    private boolean consumeFromMinEnable = true;

    private boolean consumeBroadcastEnable = true;

    private int retryQueueNums = 1;

    private int retryMaxTimes = 16;

    private long brokerId = MixAll.MASTER_ID;

    private long whichBrokerWhenConsumeSlowly = 1;

    private boolean notifyConsumerIdsChangedEnable = true;

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (brokerId ^ (brokerId >>> 32));
        result = prime * result + (consumeBroadcastEnable ? 1231 : 1237);
        result = prime * result + (consumeEnable ? 1231 : 1237);
        result = prime * result + (consumeFromMinEnable ? 1231 : 1237);
        result = prime * result + (notifyConsumerIdsChangedEnable ? 1231 : 1237);
        result = prime * result + ((groupName == null) ? 0 : groupName.hashCode());
        result = prime * result + retryMaxTimes;
        result = prime * result + retryQueueNums;
        result = prime * result + (int) (whichBrokerWhenConsumeSlowly ^ (whichBrokerWhenConsumeSlowly >>> 32));
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
        SubscriptionGroupConfig other = (SubscriptionGroupConfig) obj;
        if (brokerId != other.brokerId) {
            return false;
        }
        if (consumeBroadcastEnable != other.consumeBroadcastEnable) {
            return false;
        }
        if (consumeEnable != other.consumeEnable) {
            return false;
        }
        if (consumeFromMinEnable != other.consumeFromMinEnable) {
            return false;
        }
        if (groupName == null) {
            if (other.groupName != null) {
                return false;
            }
        } else if (!groupName.equals(other.groupName)) {
            return false;
        }
        if (retryMaxTimes != other.retryMaxTimes) {
            return false;
        }
        if (retryQueueNums != other.retryQueueNums) {
            return false;
        }
        if (whichBrokerWhenConsumeSlowly != other.whichBrokerWhenConsumeSlowly) {
            return false;
        }
        return notifyConsumerIdsChangedEnable == other.notifyConsumerIdsChangedEnable;
    }
}
