package org.apache.rocketmq.common.protocol.body;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashSet;

@Data
@EqualsAndHashCode(callSuper = false)
public class ConsumeByWho extends RemotingSerializable {

    private HashSet<String> consumedGroup = new HashSet<String>();

    private HashSet<String> notConsumedGroup = new HashSet<String>();

    private String topic;

    private int queueId;

    private long offset;
}
