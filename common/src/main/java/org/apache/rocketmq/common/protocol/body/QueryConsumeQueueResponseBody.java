package org.apache.rocketmq.common.protocol.body;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.List;

@Data
@EqualsAndHashCode(callSuper = false)
public class QueryConsumeQueueResponseBody extends RemotingSerializable {

    private SubscriptionData subscriptionData;

    private String filterData;

    private List<ConsumeQueueData> queueData;

    private long maxQueueIndex;

    private long minQueueIndex;
}
