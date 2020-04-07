package org.apache.rocketmq.common.protocol.body;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ConsumerOffsetSerializeWrapper extends RemotingSerializable {

    @Getter
    @Setter
    private ConcurrentMap<String/* topic@group */, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<String, ConcurrentMap<Integer, Long>>(512);
}
