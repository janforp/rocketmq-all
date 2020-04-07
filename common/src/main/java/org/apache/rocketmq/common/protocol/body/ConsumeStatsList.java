package org.apache.rocketmq.common.protocol.body;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = false)
public class ConsumeStatsList extends RemotingSerializable {

    private List<Map<String/*subscriptionGroupName*/, List<ConsumeStats>>> consumeStatsList = new ArrayList<Map<String/*subscriptionGroupName*/, List<ConsumeStats>>>();

    private String brokerAddr;

    private long totalDiff;
}
