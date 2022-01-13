package org.apache.rocketmq.broker.plugin;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

@Getter
@AllArgsConstructor
public class MessageStorePluginContext {

    private final MessageStoreConfig messageStoreConfig;

    private final BrokerStatsManager brokerStatsManager;

    private final MessageArrivingListener messageArrivingListener;

    private final BrokerConfig brokerConfig;
}