package org.apache.rocketmq.common.protocol.body;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashSet;
import java.util.Set;

@Data
@EqualsAndHashCode(callSuper = false)
public class LockBatchRequestBody extends RemotingSerializable {

    private String consumerGroup;

    private String clientId;

    // 要加锁的队列集合
    private Set<MessageQueue> mqSet = new HashSet<MessageQueue>();
}