package org.apache.rocketmq.common.protocol.body;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashSet;
import java.util.Set;

public class UnlockBatchRequestBody extends RemotingSerializable {

    @Setter
    @Getter
    private String consumerGroup;

    @Setter
    @Getter
    private String clientId;

    @Setter
    @Getter
    private Set<MessageQueue> mqSet = new HashSet<MessageQueue>();
}