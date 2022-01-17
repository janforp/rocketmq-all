package org.apache.rocketmq.common.protocol.heartbeat;

import lombok.Data;

@Data
public class ProducerData {

    // 生产者组名称
    private String groupName;
}