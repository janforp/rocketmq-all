package org.apache.rocketmq.common.protocol.heartbeat;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Message model
 */
@AllArgsConstructor
public enum MessageModel {

    /**
     * broadcast - 广播
     */
    BROADCASTING("BROADCASTING"),

    /**
     * clustering - 集群
     *
     * socinscore1
     * socinscore2
     * socinscore3
     *
     * 三台服务都属于同一个集群(属于同一个consumerGroup)，他们都订阅了topic(主题)：multiple.task.topic
     *
     * 当消息生产的时候，在集群模式下，这3台服务只有一台服务能消费这个消息
     */
    CLUSTERING("CLUSTERING");

    @Getter
    private final String modeCN;
}