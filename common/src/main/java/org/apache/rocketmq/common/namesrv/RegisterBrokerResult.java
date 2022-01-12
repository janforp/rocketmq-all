package org.apache.rocketmq.common.namesrv;

import lombok.Data;
import org.apache.rocketmq.common.protocol.body.KVTable;

@Data
public class RegisterBrokerResult {

    private String haServerAddr;

    private String masterAddr;

    // 里面封装了一个 hashMap
    private KVTable kvTable;
}