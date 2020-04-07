package org.apache.rocketmq.common.protocol.body;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashMap;

public class KVTable extends RemotingSerializable {

    @Getter
    @Setter
    private HashMap<String, String> table = new HashMap<String, String>();
}
