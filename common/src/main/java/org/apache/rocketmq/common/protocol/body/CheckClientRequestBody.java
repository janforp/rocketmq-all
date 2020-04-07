package org.apache.rocketmq.common.protocol.body;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class CheckClientRequestBody extends RemotingSerializable {

    @Getter
    @Setter
    private String clientId;

    @Getter
    @Setter
    private String group;

    @Getter
    @Setter
    private SubscriptionData subscriptionData;
}
