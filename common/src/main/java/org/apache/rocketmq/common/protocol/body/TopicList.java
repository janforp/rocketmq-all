package org.apache.rocketmq.common.protocol.body;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashSet;
import java.util.Set;

public class TopicList extends RemotingSerializable {

    @Setter
    @Getter
    private Set<String> topicList = new HashSet<String>();

    @Setter
    @Getter
    private String brokerAddr;
}