package org.apache.rocketmq.store.schedule;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DelayOffsetSerializeWrapper extends RemotingSerializable {

    @Getter
    @Setter
    private ConcurrentMap<Integer /* level 延迟级别*/, Long/* offset 偏移量*/> offsetTable = new ConcurrentHashMap<>(32);
}