package org.apache.rocketmq.common.admin;

import lombok.Data;

@Data
public class OffsetWrapper {

    private long brokerOffset;

    private long consumerOffset;

    private long lastTimestamp;
}
