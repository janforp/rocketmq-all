package org.apache.rocketmq.client.consumer.listener;

public enum ConsumeConcurrentlyStatus {

    /**
     * Success consumption
     * 消费成功
     */
    CONSUME_SUCCESS,
    /**
     * Failure consumption,later try to consume
     * 消费失败
     */
    RECONSUME_LATER;
}