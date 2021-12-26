package org.apache.rocketmq.client.impl;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class FindBrokerResult {

    // 地址
    private final String brokerAddr;

    // 节点角色，是否从节点
    private final boolean slave;

    // 主机的版本
    private final int brokerVersion;

    public FindBrokerResult(String brokerAddr, boolean slave) {
        this.brokerAddr = brokerAddr;
        this.slave = slave;
        this.brokerVersion = 0;
    }
}