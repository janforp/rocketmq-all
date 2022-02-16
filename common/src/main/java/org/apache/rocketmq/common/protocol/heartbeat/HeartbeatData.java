package org.apache.rocketmq.common.protocol.heartbeat;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashSet;
import java.util.Set;

@Setter
@Getter
@ToString
public class HeartbeatData extends RemotingSerializable {

    // ip&pid，如：10.201.13.28@9738
    private String clientID;

    // 生产者
    private Set<ProducerData/*groupName*/> producerDataSet/*当前jvm所有生产者数据*/ = new HashSet<ProducerData>();

    // 消费者
    private Set<ConsumerData> consumerDataSet = new HashSet<ConsumerData>();
}