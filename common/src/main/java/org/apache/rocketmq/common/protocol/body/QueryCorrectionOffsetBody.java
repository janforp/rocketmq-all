package org.apache.rocketmq.common.protocol.body;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashMap;
import java.util.Map;

public class QueryCorrectionOffsetBody extends RemotingSerializable {

    @Setter
    @Getter
    private Map<Integer, Long> correctionOffsets = new HashMap<Integer, Long>();
}
