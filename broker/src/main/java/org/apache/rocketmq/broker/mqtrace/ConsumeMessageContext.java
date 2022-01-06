package org.apache.rocketmq.broker.mqtrace;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.util.Map;

@Getter
@Setter
public class ConsumeMessageContext {

    private String consumerGroup;

    private String topic;

    private Integer queueId;

    private String clientHost;

    private String storeHost;

    private Map<String, Long> messageIds;

    private int bodyLength;

    private boolean success;

    private String status;

    private Object mqTraceContext;

    private String commercialOwner;

    private BrokerStatsManager.StatsType commercialRcvStats;

    private int commercialRcvTimes;

    private int commercialRcvSize;

    private String namespace;
}
