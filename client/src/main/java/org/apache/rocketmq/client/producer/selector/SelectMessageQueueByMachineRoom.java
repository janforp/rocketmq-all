package org.apache.rocketmq.client.producer.selector;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;
import java.util.Set;

public class SelectMessageQueueByMachineRoom implements MessageQueueSelector {

    @Getter
    @Setter
    private Set<String> consumeridcs;

    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        return null;
    }
}
