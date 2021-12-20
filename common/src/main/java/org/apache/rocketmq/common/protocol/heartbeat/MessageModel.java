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
     */
    CLUSTERING("CLUSTERING");

    @Getter
    private final String modeCN;
}