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
     * @param consumerGroup current consumer group
     * @param currentCID current consumer id
     * @param mqAll message queue set in current topic
     * @param cidAll consumer set in current consumer group
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