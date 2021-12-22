package org.apache.rocketmq.common.protocol.heartbeat;

import java.util.HashSet;
import java.util.Set;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

@ToString
@Getter
@Setter
public class ConsumerData {

    private String groupName;

    private ConsumeType consumeType;

    private MessageModel messageModel;

    private ConsumeFromWhere consumeFromWhere;

    private Set<SubscriptionData> subscriptionDataSet = new HashSet<SubscriptionData>();

    private boolean unitMode;
}