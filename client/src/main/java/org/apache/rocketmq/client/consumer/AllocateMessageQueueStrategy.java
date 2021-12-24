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
     * @param consumerGroup 消费者组
     * @param currentCID 当前需要分配队列的消费者id
     * @param mqAll 主题下的所有队列
     * @param cidAll 当前消费者组下的所有消费者实例id集合
     * @return 分配队列结果
     */
    List<MessageQueue> allocate(final String consumerGroup, final String currentCID, final List<MessageQueue> mqAll, final List<String> cidAll);

    /**
     * Algorithm name
     *
     * @return The strategy name
     */
    String getName();
}