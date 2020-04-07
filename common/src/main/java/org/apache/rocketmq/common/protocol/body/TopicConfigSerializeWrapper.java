package org.apache.rocketmq.common.protocol.body;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TopicConfigSerializeWrapper extends RemotingSerializable {

    @Setter
    @Getter
    private ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<String, TopicConfig>();

    @Setter
    @Getter
    private DataVersion dataVersion = new DataVersion();
}
