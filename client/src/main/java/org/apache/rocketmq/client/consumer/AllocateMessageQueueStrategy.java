package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

/**
 * Strategy Algorithm for message allocating between consumers
 */
public interface AllocateMessageQueueStrategy {

    /**
     * Allocating by consumer id
     *
     * @param consumerGroup current consumer group 消费者
     * @param currentCID current consumer id 当前消费者id
     * @param mqAll message queue set in current topic topic下的所有队列
     * @param cidAll consumer set in current consumer group 所有消费者实例集合
     * @return The allocate result of given strategy
     */
    List<MessageQueue> allocate(final String consumerGroup, final String currentCID, final List<MessageQueue> mqAll, final List<String> cidAll);

    /**
     * Algorithm name
     *
     * @return The strategy name
     */
    String getName();
}