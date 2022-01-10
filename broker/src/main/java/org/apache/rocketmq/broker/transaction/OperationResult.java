package org.apache.rocketmq.broker.transaction;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.message.MessageExt;

@Getter
@Setter
public class OperationResult {

    private MessageExt prepareMessage;

    private int responseCode;

    private String responseRemark;
}
