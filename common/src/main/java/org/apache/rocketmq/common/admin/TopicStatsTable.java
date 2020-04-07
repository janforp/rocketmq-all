package org.apache.rocketmq.common.admin;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashMap;

public class TopicStatsTable extends RemotingSerializable {

    @Setter
    @Getter
    private HashMap<MessageQueue, TopicOffset> offsetTable = new HashMap<MessageQueue, TopicOffset>();
}
