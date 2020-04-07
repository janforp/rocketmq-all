package org.apache.rocketmq.common.protocol.body;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

@Data
@EqualsAndHashCode(callSuper = false)
public class ConsumeMessageDirectlyResult extends RemotingSerializable {

    private boolean order = false;

    private boolean autoCommit = true;

    private CMResult consumeResult;

    private String remark;

    private long spentTimeMills;

    @Override
    public String toString() {
        return "ConsumeMessageDirectlyResult [order=" + order + ", autoCommit=" + autoCommit
                + ", consumeResult=" + consumeResult + ", remark=" + remark + ", spentTimeMills="
                + spentTimeMills + "]";
    }
}
