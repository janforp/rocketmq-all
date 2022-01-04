package org.apache.rocketmq.client.consumer.listener;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Consumer concurrent consumption context
 */
public class ConsumeConcurrentlyContext {

    // 消息归属队列
    @Getter
    private final MessageQueue messageQueue;

    /**
     * 消息失败的时候该字段才有用
     * 控制消息回退的时候，延迟级别！
     *
     * -1:直接扔进死信队列
     * 0:broker 控制延迟级别
     * >0:客户端控制延迟级别
     *
     * Message consume retry strategy<br>
     * -1,no retry,put into DLQ directly<br>
     * 0,broker control retry frequency<br>
     * >0,client control retry frequency
     */
    @Getter
    @Setter
    private int delayLevelWhenNextConsume = 0;

    // ？
    @Getter
    @Setter
    private int ackIndex = Integer.MAX_VALUE;

    public ConsumeConcurrentlyContext(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }
}