package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import lombok.AllArgsConstructor;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.net.Broker2Client;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

import java.util.Collection;
import java.util.List;

@AllArgsConstructor
public class DefaultConsumerIdsChangeListener implements ConsumerIdsChangeListener {

    private final BrokerController brokerController;

    @Override
    public void handle(ConsumerGroupEvent event, String group, Object... args) {
        if (event == null) {
            return;
        }
        switch (event) {
            case CHANGE:
                if (args == null || args.length < 1) {
                    return;
                }
                @SuppressWarnings("unchecked")
                List<Channel> channels = (List<Channel>) args[0];

                BrokerConfig brokerConfig = brokerController.getBrokerConfig();
                if (channels != null && brokerConfig.isNotifyConsumerIdsChangedEnable()) {
                    for (Channel chl : channels) {
                        Broker2Client broker2Client = brokerController.getBroker2Client();
                        broker2Client.notifyConsumerIdsChanged(chl, group);
                    }
                }
                break;
            case UNREGISTER:
                ConsumerFilterManager consumerFilterManager = this.brokerController.getConsumerFilterManager();
                consumerFilterManager.unRegister(group);
                break;
            case REGISTER:
                if (args == null || args.length < 1) {
                    return;
                }
                @SuppressWarnings("unchecked")
                Collection<SubscriptionData> subscriptionDataList = (Collection<SubscriptionData>) args[0];
                ConsumerFilterManager filterManager = this.brokerController.getConsumerFilterManager();
                filterManager.register(group, subscriptionDataList);
                break;
            default:
                throw new RuntimeException("Unknown event " + event);
        }
    }
}
