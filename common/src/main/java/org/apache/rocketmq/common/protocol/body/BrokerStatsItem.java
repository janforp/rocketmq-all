package org.apache.rocketmq.common.protocol.body;

import lombok.Data;

@Data
public class BrokerStatsItem {

    private long sum;

    private double tps;

    private double avgpt;
}
