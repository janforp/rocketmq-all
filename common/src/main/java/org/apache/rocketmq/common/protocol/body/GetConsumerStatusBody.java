package org.apache.rocketmq.common.protocol.body;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashMap;
import java.util.Map;

@Deprecated
@Data
@EqualsAndHashCode(callSuper = false)
public class GetConsumerStatusBody extends RemotingSerializable {

    private Map<MessageQueue, Long> messageQueueTable = new HashMap<MessageQueue, Long>();

    private Map<String, Map<MessageQueue, Long>> consumerTable = new HashMap<String, Map<MessageQueue, Long>>();
}
