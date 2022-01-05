package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import lombok.AllArgsConstructor;
import org.apache.rocketmq.broker.BrokerController;
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
                if (channels != null && brokerController.getBrokerConfig().isNotifyConsumerIdsChangedEnable()) {
                    for (Channel chl : channels) {
                        this.brokerController.getBroker2Client().notifyConsumerIdsChanged(chl, group);
                    }
                }
                break;
            case UNREGISTER:
                this.brokerController.getConsumerFilterManager().unRegister(group);
                break;
            case REGISTER:
                if (args == null || args.length < 1) {
                    return;
                }
                @SuppressWarnings("unchecked")
                Collection<SubscriptionData> subscriptionDataList = (Collection<SubscriptionData>) args[0];
                this.brokerController.getConsumerFilterManager().register(group, subscriptionDataList);
                break;
            default:
                throw new RuntimeException("Unknown event " + event);
        }
    }
}
