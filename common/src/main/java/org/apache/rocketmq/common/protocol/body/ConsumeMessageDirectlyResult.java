package org.apache.rocketmq.common.protocol.body;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

@Data
@EqualsAndHashCode(callSuper = false)
@ToString
public class ConsumeMessageDirectlyResult extends RemotingSerializable {

    private boolean order = false;

    private boolean autoCommit = true;

    private CMResult consumeResult;

    private String remark;

    private long spentTimeMills;
}
