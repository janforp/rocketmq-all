package org.apache.rocketmq.common.protocol.body;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SubscriptionGroupWrapper extends RemotingSerializable {

    @Setter
    @Getter
    private ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroupTable = new ConcurrentHashMap<String, SubscriptionGroupConfig>(1024);

    @Setter
    @Getter
    private DataVersion dataVersion = new DataVersion();
}
