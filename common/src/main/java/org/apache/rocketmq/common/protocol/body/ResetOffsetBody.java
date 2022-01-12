package org.apache.rocketmq.common.protocol.body;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.Map;

public class ResetOffsetBody extends RemotingSerializable {

    @Getter
    @Setter
    private Map<MessageQueue, Long> offsetTable;
}