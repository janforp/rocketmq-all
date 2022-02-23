package org.apache.rocketmq.common.protocol.heartbeat;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum ConsumeType {

    /**
     * 积极消费
     * 主动去拉
     */
    CONSUME_ACTIVELY("PULL"),

    /**
     * 被动消费
     *
     * 被动推过来
     */
    CONSUME_PASSIVELY("PUSH");

    private final String typeCN;
}