package org.apache.rocketmq.common.protocol.body;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Data
@EqualsAndHashCode(callSuper = false)
public class ConsumerConnection extends RemotingSerializable {

    private HashSet<Connection> connectionSet = new HashSet<Connection>();

    private ConcurrentMap<String/* Topic */, SubscriptionData> subscriptionTable = new ConcurrentHashMap<String, SubscriptionData>();

    private ConsumeType consumeType;

    private MessageModel messageModel;

    private ConsumeFromWhere consumeFromWhere;

    public int computeMinVersion() {
        int minVersion = Integer.MAX_VALUE;
        for (Connection c : this.connectionSet) {
            if (c.getVersion() < minVersion) {
                minVersion = c.getVersion();
            }
        }

        return minVersion;
    }
}
