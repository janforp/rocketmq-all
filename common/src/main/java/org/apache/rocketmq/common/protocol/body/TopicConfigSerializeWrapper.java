package org.apache.rocketmq.common.protocol.body;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * 记录当前节点有多少topic(主题)
 * 当前主题在该节点有多少队列
 */
public class TopicConfigSerializeWrapper extends RemotingSerializable {

    /**
     * key：topic
     * value:TopConfig
     */
    @Setter
    @Getter
    private ConcurrentMap<String/*topic*/, TopicConfig> topicConfigTable = new ConcurrentHashMap<String, TopicConfig>();

    @Setter
    @Getter
    private DataVersion dataVersion = new DataVersion();
}
