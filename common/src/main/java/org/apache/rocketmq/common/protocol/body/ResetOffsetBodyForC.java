package org.apache.rocketmq.common.protocol.body;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.message.MessageQueueForC;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.List;

public class ResetOffsetBodyForC extends RemotingSerializable {

    @Getter
    @Setter
    private List<MessageQueueForC> offsetTable;
}
