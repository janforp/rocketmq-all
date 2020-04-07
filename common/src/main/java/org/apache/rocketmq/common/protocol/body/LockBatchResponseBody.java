package org.apache.rocketmq.common.protocol.body;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashSet;
import java.util.Set;

public class LockBatchResponseBody extends RemotingSerializable {

    @Getter
    @Setter
    private Set<MessageQueue> lockOKMQSet = new HashSet<MessageQueue>();
}
