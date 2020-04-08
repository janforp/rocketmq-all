package org.apache.rocketmq.client.consumer.rebalance;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

public class AllocateMessageQueueByConfig implements AllocateMessageQueueStrategy {

    @Getter
    @Setter
    private List<MessageQueue> messageQueueList;

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
            List<String> cidAll) {
        return this.messageQueueList;
    }

    @Override
    public String getName() {
        return "CONFIG";
    }
}
