package org.apache.rocketmq.common.protocol.body;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashSet;

public class ProducerConnection extends RemotingSerializable {

    @Setter
    @Getter
    private HashSet<Connection> connectionSet = new HashSet<Connection>();
}
