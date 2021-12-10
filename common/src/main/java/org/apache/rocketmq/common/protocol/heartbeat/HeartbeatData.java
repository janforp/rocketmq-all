package org.apache.rocketmq.common.protocol.heartbeat;

import java.util.HashSet;
import java.util.Set;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

@Setter
@Getter
public class HeartbeatData extends RemotingSerializable {
    // ip&pid
    private String clientID;

    // 生产者
    private Set<ProducerData> producerDataSet = new HashSet<ProducerData>();

    // 消费者
    private Set<ConsumerData> consumerDataSet = new HashSet<ConsumerData>();


    @Override
    public String toString() {
        return "HeartbeatData [clientID=" + clientID + ", producerDataSet=" + producerDataSet + ", consumerDataSet=" + consumerDataSet + "]";
    }
}
