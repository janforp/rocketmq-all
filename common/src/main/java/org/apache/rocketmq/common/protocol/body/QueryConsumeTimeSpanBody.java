package org.apache.rocketmq.common.protocol.body;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.ArrayList;
import java.util.List;

public class QueryConsumeTimeSpanBody extends RemotingSerializable {

    @Getter
    @Setter
    List<QueueTimeSpan> consumeTimeSpanSet = new ArrayList<QueueTimeSpan>();
}
