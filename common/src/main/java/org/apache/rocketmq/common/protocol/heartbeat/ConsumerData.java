package org.apache.rocketmq.common.protocol.heartbeat;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

import java.util.HashSet;
import java.util.Set;

@ToString
@Getter
@Setter
public class ConsumerData {

    private String groupName;

    private ConsumeType consumeType;// push or pull

    private MessageModel messageModel; // 广播 or 集群

    private ConsumeFromWhere consumeFromWhere; // 开始消费的地方

    private Set<SubscriptionData> subscriptionDataSet = new HashSet<SubscriptionData>(); // 消费者的订阅信息

    private boolean unitMode;
}